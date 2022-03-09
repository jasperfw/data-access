<?php

namespace JasperFW\DataAccess\ResultSet;

use JasperFW\DataAccess\DAO;
use JasperFW\DataAccess\Exception\DatabaseQueryException;
use JetBrains\PhpStorm\Pure;
use PDO;
use PDOStatement;
use Psr\Log\LoggerInterface;

/**
 * Class ResultSetPDO
 *
 * Wrapper for PDOStatememnt objects to allow the results to be retrieved in a consistent manner.
 *
 * @package JasperFW\DataAccess\ResultSet
 */
class ResultSetPDO extends ResultSet
{
    /**
     * @param PDOStatement         $object
     * @param DAO                  $dbc
     * @param null|LoggerInterface $logger
     */
    #[Pure] public function __construct(PDOStatement $object, DAO $dbc, ?LoggerInterface $logger = null)
    {
        parent::__construct($object, $dbc, $logger);
        $this->result = $object;
        $this->pointer = 0;
    }

    /**
     * Rerun a query using the prepared statement. This is more efficent than creating new prepared statements when
     * doing bulk operations.
     *
     * @param array $params
     *
     * @return ResultSet
     * @throws DatabaseQueryException
     */
    public function execute(array $params): ResultSet
    {
        $this->logger->info('DB PREPARE: Preparing query.');
        $test_query = $this->result->queryString;
        foreach ($params as $name => $val) {
            $test_query = str_replace($name, '\'' . str_replace("'", "''", $val) . '\'', $test_query);
        }
        $this->logger->info('DB QUERY: ' . $this->result->queryString);
        $this->result->execute($params);
        if (null === $this->result->errorCode()) {
            // Something really bad happened
            $this->logger->info('PARAMS: ' . implode('|', $params));
            $this->logger->error('DB ERROR: The query could not be executed - unknown error.');
            $this->logger->debug('Test Query: ' . $test_query);
            throw new DatabaseQueryException($this->result->errorInfo()[2]);
        } elseif (preg_match('/^00000$/', $this->result->errorCode())) {
            // The query was successful
            $this->logger->debug('DB Success: ' . implode('|', $params));
        } elseif (preg_match('/^(00|01)/', $this->result->errorCode())) {
            // A warning was thrown
            $this->logger->info('PARAMS: ' . implode('|', $params));
            $this->logger->warning('DB WARN: ' . $this->result->errorCode() . ' -- ' . $this->result->errorInfo()[2]);
            $this->logger->debug('Test Query: ' . $test_query);
        } else {
            // The query failed
            $this->logger->info('PARAMS: ' . implode('|', $params));
            $this->logger->error('DB ERROR: ' . $this->result->errorCode() . ' -- ' . $this->result->errorInfo()[2]);
            $this->logger->debug('Test Query: ' . $test_query);
            throw new DatabaseQueryException($this->result->errorInfo()[2]);
        }
        return $this;
    }

    /**
     * Returns the current element in the result set as an array
     */
    public function current(): array
    {
        return $this->result->fetch(PDO::FETCH_ASSOC, PDO::FETCH_ORI_ABS, $this->pointer);
    }

    /**
     * Return the current element and advances the pointer to the next element. This should be used for iterating over
     * the result set and should not be mixed with use of current().
     *
     * @return array|bool Returns the current result row.
     */
    public function fetch(): array|bool
    {
        return $this->result->fetch(PDO::FETCH_ASSOC);
    }

    /**
     * Checks if current position is valid
     *
     * @return boolean The return value will be casted to boolean and then evaluated.
     *       Returns true on success or false on failure.
     */
    public function valid(): bool
    {
        if (false === $this->result->fetch(PDO::FETCH_ASSOC, PDO::FETCH_ORI_ABS, $this->pointer)) {
            return false;
        }
        return true;
    }

    /**
     * Converts the full result set to an array. This can be very memory intensive, especially for large result sets
     * and therefore is not typically recommended.
     */
    public function toArray(): array
    {
        return $this->result->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Returns the number of rows affected by the statement if it was a DELETE, INSERT or UPDATE. May return the number
     * of rows in a result set as well, but this is unpredictable.
     *
     * @return int The number of rows
     */
    public function numRows(): int
    {
        return $this->result->rowCount();
    }
}
