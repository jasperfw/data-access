<?php

namespace JasperFW\DataAccess\ResultSet;

use JasperFW\DataAccess\Exception\DatabaseQueryException;

/**
 * Class ResultSetArray
 *
 * This class wraps result sets that are already arrays so that they can be accessed in a manner consistent with other
 * result types, such as PDOStatement objects. Arrays are returned by Dummy, used for unit testing, as well as LDAP.
 *
 * @package JasperFW\DataAccess\ResultSet
 */
class ResultSetArray extends ResultSet
{
    /**
     * Rerun a query using the prepared statement. This is more efficent than creating new prepared statements when
     * doing bulk operations.
     *
     * @param $params
     *
     * @return ResultSet|void
     * @throws DatabaseQueryException
     */
    public function execute(array $params): ?ResultSet
    {
        // This is not supported
        return $this;
    }

    /**
     * Return the current element
     *
     * @return array Returns the current result row.
     */
    public function current(): array
    {
        return $this->result[$this->pointer];
    }

    /**
     * Return the current element and advances the pointer to the next element. This should be used for iterating over
     * the result set and should not be mixed with use of current().
     *
     * @return array|bool Returns the current result row.
     */
    public function fetch()
    {
        return $this->result[$this->pointer++] ?? false;
    }

    /**
     * Checks if current position is valid
     *
     * @return boolean The return value will be casted to boolean and then evaluated.
     *       Returns true on success or false on failure.
     */
    public function valid(): bool
    {
        return isset($this->result[$this->pointer]);
    }

    /**
     * Converts the full result set to an array. This can be very memory intensive, especially for large result sets
     * and therefore is not typically recommended.
     *
     * @return array
     */
    public function toArray(): array
    {
        return $this->result;
    }

    /**
     * Returns the number of rows affected by the statement if it was a DELETE, INSERT or UPDATE. May return the number
     * of rows in a result set as well, but this is unpredictable.
     *
     * @return int The number of rows
     */
    public function numRows(): int
    {
        return count($this->result);
    }
}