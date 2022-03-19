<?php

namespace JasperFW\DataAccess;

use Exception;
use JasperFW\DataAccess\Exception\DatabaseConnectionException;
use JasperFW\DataAccess\Exception\DatabaseQueryException;
use JasperFW\DataAccess\Exception\TransactionErrorException;
use JasperFW\DataAccess\Exception\TransactionsNotSupportedException;
use JasperFW\DataAccess\ResultSet\ResultSet;
use JasperFW\DataAccess\ResultSet\ResultSetPDO;
use JetBrains\PhpStorm\Pure;
use PDO;
use PDOException;
use PDOStatement;
use Psr\Log\LoggerInterface;

/**
 * Class MsSQL
 *
 * Handle MS SQL server data with PDO requests. This is designed to work with MSSQL 2012-2014 instead of MSSQL 2000
 *
 * @package JasperFW\DataAccess
 */
class MsSQL extends DAO
{
    /** @var PDO|null connection to server */
    protected ?PDO $dbconn;
    /** @var ResultSetPDO|null Returned query object */
    protected ?ResultSet $stmt = null;
    /** @var bool True if the last query attempted succeeded */
    protected $querySucceeded = false;

    /**
     * Escapes the column names. Generally this is done by surrounding the column name with backticks.
     *
     * @param string $column_name The name of the column to escape
     *
     * @return string
     * @deprecated because mocking issues
     */
    public static function escapeColumnName(string $column_name): string
    {
        return '[' . $column_name . ']';
    }

    /**
     * Generates the object. This does not connect to the server - that should be done only by the query function so
     * that connections are only loaded if they are being used.
     *
     * @param array                $config Configuration settings for the connection this object represents
     * @param LoggerInterface|null $logger The log system
     *
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
     * Establish a connection to the database server.
     *
     * @return mixed
     * @throws DatabaseConnectionException
     */
    public function connect(): bool
    {
        try {
            $connection_string = 'sqlsrv:SERVER=' .
                $this->configuration['server'] .
                ';DATABASE=' .
                $this->configuration['dbname'];
            $this->logger->debug('Connecting to database engine.');
            $this->dbconn = new PDO(
                $connection_string, $this->configuration['username'],
                $this->configuration['password']
            );
            $this->dbconn->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
            $this->logger->debug('Connected to database engine.');
            $this->isConnected = true;
        } catch (Exception $e) {
            throw new DatabaseConnectionException(
                'MSSQL Connection to <strong>' .
                $this->configuration['server'] .
                '</strong> failed!<br>' .
                $e->getMessage()
            );
        }
        return true;
    }

    /**
     * Close the connection if it has been established.
     */
    public function disconnect(): void
    {
        if ($this->dbconn->inTransaction()) {
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
        } catch (PDOException) {
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
    #[Pure] public function querySucceeded(): bool
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
     * @return int|null
     */
    public function lastInsertId(string $name = null): ?int
    {
        return $this->dbconn->lastInsertId($name);
    }

    /**
     * Escapes the passed column name according to the rules for the database engine. This replaces the static method
     * because unit tests can't mock the static method.
     *
     * @param string $columnName
     *
     * @return string
     */
    public function escapeColName(string $columnName): string
    {
        return '[' . $columnName . ']';
    }
}
