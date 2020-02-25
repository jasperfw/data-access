<?php

namespace JasperFW\DataAccess;

use Exception;
use JasperFW\DataAccess\Exception\DatabaseConnectionException;
use JasperFW\DataAccess\Exception\DatabaseQueryException;
use JasperFW\DataAccess\ResultSet\ResultSet;
use JasperFW\DataAccess\ResultSet\ResultSetArray;
use PDOStatement;
use Psr\Log\LoggerInterface;

/**
 * Class Dummy
 *
 * The dummy class is designed to replace a normal database connection. Test data can be stored in the object as an
 * array. This class is intended to be used ONLY in unit testing.
 *
 * To simulate a database connection failure, call setFailureConnection(true) before the operation is attempted.
 *
 * To simulate a query failure, call the setFailureQuery(true) before the operation is attempted.
 *
 * @package JasperFW\DataModeling\DataAccess
 */
class Dummy extends DAO
{
    public $query = null;
    public $params = null;
    private $querySucceeded = false;
    private $testData = null;
    private $result = null;
    private $failConnectionFailed = false;
    private $failQueryFailed = false;

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
     * @param array           $config Configuration settings for the connection this object represents
     * @param LoggerInterface $logger Logging system
     *
     * @throws Exception
     */
    public function __construct(array $config = [], LoggerInterface $logger = null)
    {
        parent::__construct($config, $logger);
        $this->testData = null;
        if (isset($config['testdata'])) {
            $this->testData = $config['testdata'];
        }
    }

    /**
     * Testing method - set that the connection failed to test error handling. Set this before attempting the query.
     *
     * @param boolean $failConnectionFailed
     */
    public function setFailConnectionFailed(bool $failConnectionFailed = true): void
    {
        $this->is_connected = false;
        $this->failConnectionFailed = $failConnectionFailed;
    }

    /**
     * Testing method - set that the query failed to test error handling. Set this before attempting the query.
     *
     * @param boolean $failQueryFailed
     */
    public function setFailQueryFailed(bool $failQueryFailed = true): void
    {
        $this->querySucceeded = false;
        $this->failQueryFailed = $failQueryFailed;
    }

    /**
     * Testing method - This is a cheat method to insert test data. This is to be used by PHPUnit and other test
     * applications.
     *
     * @param array $testData
     */
    public function setTestData(array $testData): void
    {
        $this->testData = $testData;
    }

    /**
     * Establish a connection to the database server. If setFailConnectionFailed is true, this will return an exception
     * as if the connection to a database server had failed.
     *
     * @return bool True if the connection succeeds
     * @throws DatabaseConnectionException
     */
    public function connect(): bool
    {
        if ($this->failConnectionFailed) {
            throw new DatabaseConnectionException('Unable to connect to the database!');
        }
        $this->is_connected = true;
        return true;
    }

    /**
     * Close the connection if it has been established.
     *
     * @return bool True if the disconnection is successful
     */
    public function disconnect(): bool
    {
        $this->is_connected = false;
        return true;
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
     * @throws DatabaseConnectionException
     * @throws DatabaseQueryException
     */
    public function query(string $query_string, array $params = [], array $options = []): DAO
    {
        $this->query = $query_string;
        if (isset($options['params'])) {
            $this->params = $options['params'];
        }
        if (!$this->is_connected) {
            $this->connect();
        }
        if ($this->failQueryFailed) {
            throw new DatabaseQueryException('Unable to complete the query');
        }
        if (isset($options['testdata'])) {
            $this->testData = $options['testdata'];
        }
        if (null != $this->testData) {
            $this->querySucceeded = true;
        }
        $this->result = new ResultSetArray($this->testData, $this, $this->logger);
        return $this;
    }

    /**
     * Returns true if the previous query succeeded
     *
     * @return bool True if the last query worked.
     */
    public function querySucceeded(): bool
    {
        return $this->querySucceeded;
    }

    /**
     * Gets the results and a multidimensional array.
     *
     * @return array|null The results as an array or null if this was not a select or if there was an error.
     */
    public function toArray(): ?array
    {
        if ($this->querySucceeded) {
            return $this->testData;
        } else {
            return null;
        }
    }

    /**
     * If the query was successful, return the result resource object.
     *
     * @return mixed
     */
    public function getResult(): PDOStatement
    {
        return $this->result;
    }

    /**
     * Get the last inserted id for the last query run on this connection.
     *
     * @return int 30. For testing purposes, its 30.
     */
    public function lastInsertId(): int
    {
        return 30;
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
        return '[' . $column_name . ']';
    }

    /**
     * Use this function to free up resources and perform any required cleanup when the dao is released.
     */
    public function __destruct()
    {
        $this->is_connected = false;
        $this->querySucceeded = false;
        $this->testData = null;
    }

    /**
     * Returns a statement object representing a prepared statement for the database.
     *
     * @param string $query_string The query
     *
     * @return ResultSet
     */
    public function getStatement(string $query_string): ResultSet
    {
        return new ResultSetArray($this->testData, $this, $this->logger);
    }

    /**
     * Begin a transaction.
     *
     * @return bool True if the operation succeeds
     */
    public function beginTransaction(): bool
    {
        return true;
    }

    /**
     * Rollback a transaction.
     *
     * @return bool True if the operation succeeds
     */
    public function rollbackTransaction(): bool
    {
        return true;
    }

    /**
     * Commit the transaction.
     *
     * @return bool True if the operation succeeds
     */
    public function commitTransaction(): bool
    {
        return true;
    }

    /**
     * Checks the configuration array for required elements. Since this dummy class doesn't actually connect to anything
     * there are no required configuration fields.
     *
     * @param $config
     *
     * @return bool
     */
    protected function validateConfiguration(array $config): bool
    {
        return true;
    }
}