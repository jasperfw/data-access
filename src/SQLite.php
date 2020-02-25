<?php

namespace JasperFW\DataAccess;

use Exception;
use JasperFW\DataAccess\Exception\DatabaseConnectionException;
use JasperFW\DataAccess\Exception\DatabaseQueryException;
use JasperFW\DataAccess\Exception\TransactionErrorException;
use JasperFW\DataAccess\Exception\TransactionsNotSupportedException;
use JasperFW\DataAccess\ResultSet\ResultSet;
use JasperFW\DataAccess\ResultSet\ResultSetPDO;
use PDO;
use PDOException;
use PDOStatement;
use Psr\Log\LoggerInterface;

class SQLite extends DAO
{
    /** @var PDO connection to server */
    private $dbconn;
    /** @var null|ResultSetPDO Returned query results */
    private $stmt;
    /** @var bool True if the query succeeded, false otherwise */
    private $querySucceeded = false;

    /**
     * SQLite constructor.
     *
     * @param array           $config
     * @param LoggerInterface $logger
     *
     * @throws Exception
     */
    public function __construct(array $config, LoggerInterface $logger = null)
    {
        parent::__construct($config, $logger);
    }

    public function __destruct()
    {
        $this->disconnect();
        $this->dbconn = null;
        $this->stmt = null;
        $this->querySucceeded = false;
    }

    /**
     * Create the connection to the database
     *
     * @throws DatabaseConnectionException
     */
    public function connect(): void
    {
        $dsn = 'sqlite:' . $this->configuration['server'];
        $this->logger->debug('Connecting to database engine.');
        //$user = $this->configuration['username'];
        //$pwd = $this->configuration['password'];
        try {
            $this->dbconn = new PDO($dsn);
            $this->logger->debug('Connected to database engine.');
            $this->is_connected = true;
        } catch (Exception $e) {
            var_dump($e);
            $errorMessage = 'SQLite Connection to <strong>' . $dsn . '</strong> failed!';
            $this->logger->warning($errorMessage, [$e->getMessage()]);
            throw new DatabaseConnectionException($errorMessage);
        }
    }

    /**
     * Closes the PDO statement and database connection
     */
    public function disconnect()
    {
        if ($this->is_connected && $this->dbconn->inTransaction()) {
            $this->dbconn->rollBack();
        }
        $this->dbconn = null;
        $this->is_connected = false;
    }

    /**
     * Start a transaction.
     *
     * @return bool
     * @throws TransactionErrorException if a transaction is already started
     * @throws TransactionsNotSupportedException if transactions are not supported
     */
    public function beginTransaction(): bool
    {
        if ($this->dbconn->inTransaction()) {
            throw new TransactionErrorException("A transaction has already been started.");
        }
        try {
            return $this->dbconn->beginTransaction();
        } catch (PDOException $exception) {
            throw new TransactionsNotSupportedException("Transactions are not supported in this database.");
        }
    }

    /**
     * Rollback the transaction.
     *
     * @return bool
     * @throws TransactionErrorException if there is no active transaction
     */
    public function rollbackTransaction(): bool
    {
        if (!$this->dbconn->inTransaction()) {
            throw new TransactionErrorException("There is no active transaction to roll back.");
        }
        return $this->dbconn->rollBack();
    }

    /**
     * Commit the transaction.
     *
     * @return bool
     * @throws TransactionErrorException
     */
    public function commitTransaction(): bool
    {
        if (!$this->dbconn->inTransaction()) {
            throw new TransactionErrorException("There is no active transaction to commit.");
        }
        return $this->dbconn->commit();
    }

    /**
     * Sets up the query to run. This has not been tested yet.
     *
     * @param string $query_string string to be set as prepare statment for the database. Use :{paramname}
     * @param array  $params       The parameters for the query
     * @param array  $options      driver options for the prepare statment
     *
     * @return DAO
     * @throws DatabaseQueryException*@throws DatabaseConnectionException
     * @throws DatabaseConnectionException
     */
    public function query(string $query_string, array $params = [], array $options = []): DAO
    {
        // Reset in case a previous query was attempted.
        $this->querySucceeded = false;

        // Connect to the database
        if (!$this->is_connected) {
            $this->connect();
        }

        // Get the query string and params
        $query_params = [];
        if (isset($options['params'])) {
            $query_params = $options['params'];
        }

        try {
            $stmt = $this->getStatement($query_string);
            $this->stmt = $stmt;
            $stmt->execute($query_params);
        } catch (Exception $e) {
            throw new DatabaseQueryException(
                $e->getMessage() . '|| QUERY: ' . $query_string . '|| PARAMS: ' . implode(
                    ';',
                    $query_params
                )
            );
        }

        return $this;
    }

    /**
     * Returns a statement object representing a prepared statement for the database.
     *
     * @param string $query_string The query
     *
     * @return ResultSet
     * @throws DatabaseConnectionException
     * @throws DatabaseQueryException
     */
    public function getStatement(string $query_string): ResultSet
    {
        // Connect to the database
        if (!$this->is_connected) {
            $this->connect();
        }
        $stmt = $this->dbconn->prepare($query_string);
        if (false === $stmt) {
            // The statement could not be prepared, return an appropriate exception
            throw new DatabaseQueryException('The query could not be prepared. ' . $this->dbconn->errorInfo()[2]);
        }
        return new ResultSetPDO($stmt, $this, $this->logger);
    }

    /**
     * Returns true if the previous query succeeded
     *
     * @return bool True if the last query worked.
     */
    public function querySucceeded(): bool
    {
        return $this->stmt->querySucceeded();
    }

    /**
     * Gets the results and a multidimensional array.
     *
     * @return array|null The results as an array or null if this was not a select or if there was an error.
     */
    public function toArray(): ?array
    {
        if (null === $this->stmt || false === $this->stmt) {
            return null;
        }
        return $this->stmt->toArray();
    }

    /**
     * If the query was successful, return the result resource object.
     *
     * @return mixed
     */
    public function getResult(): PDOStatement
    {
        return $this->stmt;
    }

    /**
     * This is not supported by this driver.
     *
     * @param null|string $name The name of the table
     *
     * @return int
     */
    public function lastInsertId(string $name = null): int
    {
        return $this->dbconn->lastInsertId($name);
    }

    /**
     * Checks the configuration array for required elements. By default checks the server, username and password
     * elements are set. Override to check for additional elements required for a specific database.
     *
     * @param array $config The array of configuration settings
     *
     * @return bool True if the configuration is correct
     * @throws DatabaseConnectionException if there is an error or missing field in the configuration
     */
    protected function validateConfiguration(array $config): bool
    {
        if (!is_array($config)) {
            throw new DatabaseConnectionException('The configuration is not an array.');
        }
        if (!isset($config['server'])) {
            throw new DatabaseConnectionException('The configuration does not contain a host.');
        }
        return true;
    }
}