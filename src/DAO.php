<?php

namespace JasperFW\DataAccess;

use Exception;
use JasperFW\DataAccess\Exception\DatabaseConnectionException;
use JasperFW\DataAccess\Exception\DatabaseQueryException;
use JasperFW\DataAccess\Exception\TransactionsNotSupportedException;
use JasperFW\DataAccess\ResultSet\ResultSet;
use PDOStatement;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

/**
 * Class DAO
 *
 * Parent class for database connections.
 *
 * @package JasperFW\\DataAccess
 */
abstract class DAO
{
    /** @var bool True if the connection has been established */
    protected $isConnected = false;
    /** @var array The configuration file for this database connection */
    protected $configuration;
    /** @var LoggerInterface The logger for recording errors and debug information */
    protected $logger;
    /** @var bool True if there is an active transaction */
    protected $inTransaction = false;
    /** @var bool True if the last query succeeded */
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
        return '`' . $column_name . '`';
    }

    /**
     * DAO constructor.
     *
     * @param array                $configuration The configuration for the connection
     * @param LoggerInterface|null $logger        The error and debug logger
     *
     * @throws DatabaseConnectionException if a connection to the database can not be established
     */
    public function __construct(array $configuration, ?LoggerInterface $logger = null)
    {
        $this->logger = $logger ?? new NullLogger();
        try {
            if ($this->validateConfiguration($configuration)) {
                $this->configuration = $configuration;
            }
        } catch (Exception $e) {
            $this->logger->error('A problem was found in the configuration. ' . $e->getMessage());
            throw new DatabaseConnectionException($e->getMessage());
        }
    }

    /**
     * Use this function to free up resources and perform any required cleanup when the dao is released.
     */
    abstract public function __destruct();

    /**
     * Establish a connection to the database server.
     *
     * @return mixed
     */
    abstract public function connect();

    /**
     * Close the connection if it has been established.
     */
    abstract public function disconnect();

    /**
     * Begin a transaction.
     *
     * @return bool True if the operation succeeds
     * @throws TransactionsNotSupportedException if the database engine does not support transactions
     */
    abstract public function beginTransaction(): bool;

    /**
     * Rollback a transaction.
     *
     * @return bool True if the operation succeeds
     * @throws TransactionsNotSupportedException if the database engine does not support transactions
     */
    abstract public function rollbackTransaction(): bool;

    /**
     * Commit the transaction.
     *
     * @return bool True if the operation succeeds
     * @throws TransactionsNotSupportedException if the database engine does not support transactions
     */
    abstract public function commitTransaction(): bool;

    /**
     * Check if the connection has an active transaction
     *
     * @return bool
     */
    public function inTransaction(): bool
    {
        return $this->inTransaction;
    }

    /**
     * Execute a query with the passed options. Typically the options array will include a params subarray to run the
     * query as a prepared statement.
     *
     * @param string $queryString The query to be sent
     * @param array  $params      Query parameters
     * @param array  $options     Additional arguments
     *
     * @return DAO
     * @throws DatabaseQueryException
     */
    public function query(string $queryString, array $params = [], array $options = []): self
    {
        // Reset in case a previous query was attempted.
        $this->querySucceeded = false;
        // Connect to the database
        if (!$this->isConnected) {
            $this->connect();
        }
        // Get the query string and params
        try {
            $stmt = $this->getStatement($queryString);
            $this->stmt = $stmt;
            $stmt->execute($params);
        } catch (Exception $e) {
            throw new DatabaseQueryException(
                $e->getMessage() . '|| QUERY: ' . $queryString . '|| PARAMS: ' . implode(
                    ';',
                    $params
                )
            );
        }
        return $this;
    }

    /**
     * Returns a statement object representing a prepared statement for the database.
     *
     * @param string $queryString The query
     *
     * @return ResultSet
     */
    abstract public function getStatement(string $queryString): ?ResultSet;

    /**
     * Returns true if the previous query succeeded
     *
     * @return bool True if the last query worked.
     */
    abstract public function querySucceeded(): bool;

    /**
     * Gets the results and a multidimensional array.
     *
     * @return array|null The results as an array or null if this was not a select or if there was an error.
     */
    abstract public function toArray(): ?array;

    /**
     * If the query was successful, return the result resource object.
     *
     * @return PDOStatement
     */
    abstract public function getResult(): PDOStatement;

    /**
     * Get the last inserted id for the last query run on this connection.
     */
    abstract public function lastInsertId(): ?int;

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
        return '`' . $columnName . '`';
    }

    /**
     * Take a passed parameter name and change it to a properly formatted parameter label. For 99% of database engines
     * this is done by simply putting a colon in front of the parameter name.
     *
     * @param string $parameterName The name of the parameter
     *
     * @return string The formatted label for the parameter
     */
    public function makeParameterLabel(string $parameterName): string
    {
        return ':' . ltrim($parameterName, ':'); // Prevent duplicate colons
    }

    /**
     * Check if the connection was successfully established.
     *
     * @return bool True if the connection was successfully established
     */
    public function isConnected(): bool
    {
        return $this->isConnected;
    }

    /**
     * Takes snippets of where clauses to generate an anded where clause.
     *
     * @param string[] $clauses A set of where clauses to combine with 'AND'
     * @param string   $prepend Text such as 'AND' or 'WHERE' to prepend to the clause
     *
     * @return string The generated where clause to include in the query.
     */
    public function generateWhere(?array $clauses, string $prepend = ''): string
    {
        // If no clauses are provided, return an empty string
        if (null === $clauses || count($clauses) < 1) {
            return '';
        }
        $clauses = array_values(
            array_filter(
                $clauses,
                function ($val) {
                    return $val !== '';
                }
            )
        );
        $return = implode(' AND ', $clauses);
        if ($return !== '') {
            $return = $prepend . ' ' . $return;
        }
        return $return;
    }

    /**
     * To make it easier to generate parameterized queries, this function takes a data array and returns an array with
     * four elements, the list of fields, the list of values (as params), a string of column names and values for
     * update queries, and a params array. These elements can then be combined into an insert or update query.
     *
     * @param array $data The fields to insert or update.
     *
     * @return array 'fields' is the field portion, 'values' are the values and 'params' is the param array
     */
    public function generateParameterizedComponents(array $data): array
    {
        $fields = [];
        $values = [];
        $update = [];
        //$update_string = [];
        $params = [];
        $debug = [];
        foreach ($data as $key => $item) {
            $fields[] = static::escapeColName($key);
            $p = ':' . preg_replace('/\W+/i', '_', $key);
            $values[] = $p;
            $params[$p] = $item;
            $update[] = static::escapeColName($key) . ' = ' . $p;
            //$update_string[] = static::escapeColumnName($key) . ' = ' . static::escapeString($item);
            $debug[] = $key . '=|' . $item . '|(' . gettype($item) . ')';
        }
        $fields = implode(',', $fields);
        $values = implode(',', $values);
        $update = implode(', ', $update);
        return [
            'fields' => $fields,
            'values' => $values,
            'update' => $update,
            'params' => $params,
            'debug' => implode('::', $debug),
        ];
    }

    /**
     * Returns a database engine specific pagination snippet for inclusion in a query. If the page is null, this will
     * simply return a limit without an offset. If the pageSize is null or 0, no snippet will be returned (empty string)
     * as this would cause the query to affect 0 records
     *
     * @param int $pageSize The number of results in a page
     * @param int $page     The page in the result set to request
     *
     * @return string The SQL snippet
     */
    public function generatePagination(int $pageSize = null, int $page = null): string
    {
        $paging = '';
        if (null === $pageSize || 0 == $pageSize || 1 === $pageSize) {
            return $paging;
        }
        $paging .= 'LIMIT ';
        if (null != $page && 0 != $page && 1 !== $page) {
            $offset = ($page * $pageSize) - $pageSize;
            $paging .= $offset . ', ';
        }
        return $paging . $pageSize;
    }

    /**
     * Generate a snippet of database engine specific sorting code, based on the passed $column and $direction.
     * Alternatively, an array of columns can be passed in that will be sorted according to the $direction, or an array
     * of colname => direction can be passed in.
     *
     * @param array       $columns Array of column names to sort by, where the key is the column name and the value is
     *                             the direction to sort in (ASC or DESC)
     * @param string|null $prepend String to prepend to the sort, if left blank will use "ORDER BY"
     *
     * @return string
     */
    public function generateSort(array $columns, ?string $prepend = null): string
    {
        if (empty($columns)) {
            return '';
        }
        if ($prepend === null) {
            $prepend = 'ORDER BY';
        }
        $snippets = [];
        foreach ($columns as $name => $direction) {
            $snippets[] = $name . ' ' . $direction;
        }
        return $prepend . ' ' . implode(',', $snippets);
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
        if (!isset($config['username'])) {
            throw new DatabaseConnectionException('The configuration does not contain a username.');
        }
        if (!isset($config['password'])) {
            throw new DatabaseConnectionException('The configuration does not contain a password.');
        }
        return true;
    }
}
