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
 * Class DB2
 *
 * Wrapper for DB2 connections
 *
 * TODO: This needs to be completed.
 *
 * @package JasperFW\\DataAccess
 */
class DB2 extends DAO
{
    /** @var PDO */
    protected $dbconn;
    /** @var PDOStatement */
    protected $stmt;
    protected $query_succeeded;

    /**
     * Escapes the column names. Generally this is done by surrounding the column name with backticks. DB2 doesn't
     * appear to generally escape column names.
     *
     * @param string $column_name The name of the column to escape
     *
     * @return string
     * @deprecated because mocking issues
     */
    public static function escapeColumnName(string $column_name): string
    {
        return $column_name;
    }

    /**
     * Generates the object. This does not connect to the server - that should be done only by the query function so
     * that connections are only loaded if they are being used.
     *
     * @param array           $config Configuration settings for the connection this object represents
     * @param LoggerInterface $logger Logging system
     *
     * @throws DatabaseConnectionException
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
        $this->query_succeeded = false;
    }

    /**
     * Establish a connection to the database server.
     *
     * @throws DatabaseConnectionException
     */
    public function connect(): bool
    {
        $connection_string = 'ibm:DRIVER={IBM DB2 ODBC DRIVER};HOSTNAME=' .
            $this->configuration['server'] .
            ';PORT=50000;DATABASE=' .
            $this->configuration['dbname'] .
            ';PROTOCOL=TCPIP';
        $this->logger->info('DB2 Connection String: ' . $connection_string);
        try {
            $this->logger->debug('Connecting to database engine.');
            //if (false === $this->configuration['username'] && false === $this->configuration['password']) {
            //    // This DBC uses the user's credentials
            //    $this->logger->info('Creating user-specific PDO connection');
            //    $pdo = User::i()->getSecurePDO($this, $connection_string);
            //} else {
            // This DBC uses standard credentials
            $this->logger->info('Creating PDO connection as defined in configuration.');
            $pdo = new PDO($connection_string, $this->configuration['username'], $this->configuration['password']);
            //}
            $this->logger->debug('Connected to database engine.');
            $this->dbconn = $pdo;
            $this->is_connected = true;
        } catch (Exception $e) {
            $this->logger->error('DB2 Connection failed: ' . $connection_string);
            throw new DatabaseConnectionException(
                'DB2 Connection to <strong>' .
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
    public function disconnect()
    {
        if ($this->dbconn->inTransaction()) {
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
     * Execute a query with the passed options. Typically the options array will include a params subarray to run the
     * query as a prepared statement.
     *
     * @param string $query_string
     * @param array  $params
     * @param array  $options
     *
     * @return DAO
     * @throws Exception
     */
    public function query(string $query_string, array $params = [], array $options = []): DAO
    {
        // Reset in case a previous query was attempted.
        $this->query_succeeded = false;

        // Connect to the database
        if (!$this->is_connected) {
            $this->connect();
        }

        try {
            $stmt = $this->getStatement($query_string);
            $this->stmt = $stmt;
            $stmt->execute($params);
        } catch (DatabaseQueryException $e) {
            throw $e;
        } catch (Exception $e) {
            throw new DatabaseQueryException($e->getMessage() . '|| QUERY: ' . $query_string);
        }

        $this->query_succeeded = $this->stmt->querySucceeded();

        return $this;
    }

    /**
     * Returns a statement object representing a prepared statement for the database.
     *
     * @param string $query_string The query
     *
     * @return ResultSet
     * @throws DatabaseConnectionException
     * @throws DatabaseQueryException If the statement can't be executed.
     */
    public function getStatement(string $query_string): ResultSet
    {
        if (!$this->is_connected) {
            $this->connect();
        }
        $stmt = $this->dbconn->prepare($query_string);
        if (false === $stmt) {
            // The statement could not be prepared, return an appropriate exception
            throw new DatabaseQueryException(
                'The query could not be prepared. | ' . $query_string . ' | ' . $this->dbconn->errorInfo()[2]
            );
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
     * @return PDOStatement
     */
    public function getResult(): PDOStatement
    {
        return $this->stmt;
    }

    /**
     * Get the last inserted id for the last query run on this connection.
     */
    public function lastInsertId(): int
    {
        return $this->dbconn->lastInsertId();
    }

    /**
     * Escapes the passed column name according to the rules for the database engine. This replaces the static method
     * because unit tests can't mock the static method.
     *
     * @param string $column_name
     *
     * @return string
     */
    public function escapeColName(string $column_name): string
    {
        return $column_name;
    }

    /**
     * Returns a database engine specific pagination snippet for inclusion in a query.
     *
     * @param int $page      The page in the result set to request
     * @param int $page_size The number of results in a page
     *
     * @return string
     */
    public function generatePagination(int $page = null, int $page_size = null): string
    {
        // TODO: Implement generatePagination() method.
        return '';
    }
}