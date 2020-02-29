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

/**
 * Class MySQL
 *
 * Connection and query manager for MySQL databases.
 *
 * @package JasperFW\DataAccess
 */
class MySQL extends DAO
{
    /** @var PDO connection to server */
    private $dbconn;
    /** @var null|ResultSetPDO Returned query results */
    private $stmt;
    /** @var bool True if the query succeeded, false otherwise */
    private $querySucceeded = false;

    /**
     * MySQL constructor.
     *
     * @param array           $config
     * @param LoggerInterface $logger
     *
     * @throws DatabaseConnectionException
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
        $dsn = 'mysql:host=' . $this->configuration['server'] . ';dbname=' . $this->configuration['dbname'];
        $this->logger->debug('Connecting to database engine.');
        $user = $this->configuration['username'];
        $pwd = $this->configuration['password'];
        try {
            $this->dbconn = new PDO(
                $dsn,
                $user,
                $pwd,
                [PDO::ATTR_PERSISTENT => true, PDO::ATTR_ERRMODE => PDO::ERRMODE_SILENT]
            );
            $this->logger->debug('Connected to database engine.');
            $this->isConnected = true;
        } catch (Exception $e) {
            $errorMessage = 'MySQL Connection to <strong>' . $dsn . '</strong> failed!';
            $this->logger->warning($errorMessage, [$e->getMessage()]);
            throw new DatabaseConnectionException($errorMessage);
        }
    }

    /**
     * Closes the PDO statement and database connection
     */
    public function disconnect()
    {
        if ($this->isConnected && $this->dbconn->inTransaction()) {
            $this->dbconn->rollBack();
        }
        $this->dbconn = null;
        $this->isConnected = false;
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
     * Returns a statement object representing a prepared statement for the database.
     *
     * @param string $queryString The query
     *
     * @return ResultSet
     * @throws DatabaseConnectionException
     * @throws DatabaseQueryException
     */
    public function getStatement(string $queryString): ResultSet
    {
        // Connect to the database
        if (!$this->isConnected) {
            $this->connect();
        }
        $stmt = $this->dbconn->prepare($queryString, [PDO::ATTR_CURSOR => PDO::CURSOR_SCROLL]);
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
}