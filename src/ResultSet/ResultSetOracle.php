<?php /** @noinspection PhpComposerExtensionStubsInspection */

namespace JasperFW\DataAccess\ResultSet;

use JasperFW\DataAccess\DAO;
use JasperFW\DataAccess\Exception\DatabaseQueryException;
use Psr\Log\LoggerInterface;
use Traversable;

/**
 * Class ResultSetOracle
 *
 * ResultSet for handling queries specific to the Oracle database engine
 *
 * @package JasperFW\DataAccess\ResultSet
 */
class ResultSetOracle extends ResultSet
{
    /** @var array|bool|Traversable|resource */
    protected $result;

    /**
     * @param Traversable|bool|array $object
     * @param DAO                    $dbc
     * @param null|LoggerInterface   $logger
     */
    public function __construct($object, DAO $dbc, ?LoggerInterface $logger = null)
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
        foreach ($params as $name => $value) {
            oci_bind_by_name($this->result, $name, $value);
        }
        if ($this->dbc->inTransaction()) {
            oci_execute($this->result, OCI_NO_AUTO_COMMIT);
        } else {
            oci_execute($this->result);
        }
        var_dump(oci_error());
        // TODO: Implement error logging
        // $this->result->execute($params);
        // if (null === $this->result->errorCode()) {
        //     // Something really bad happened
        //     $this->logger->info('PARAMS: ' . implode('|', $params));
        //     $this->logger->error('DB ERROR: The query could not be executed - unknown error.');
        //     $this->logger->debug('Test Query: ' . $test_query);
        //     throw new DatabaseQueryException($this->result->errorInfo()[2]);
        // } elseif (preg_match('/^00000$/', $this->result->errorCode())) {
        //     // The query was successful
        //     $this->logger->debug('DB Success: ' . implode('|', $params));
        // } elseif (preg_match('/^(00|01)/', $this->result->errorCode())) {
        //     // A warning was thrown
        //     $this->logger->info('PARAMS: ' . implode('|', $params));
        //     $this->logger->warning('DB WARN: ' . $this->result->errorCode() . ' -- ' . $this->result->errorInfo()[2]);
        //     $this->logger->debug('Test Query: ' . $test_query);
        // } else {
        //     // The query failed
        //     $this->logger->info('PARAMS: ' . implode('|', $params));
        //     $this->logger->error('DB ERROR: ' . $this->result->errorCode() . ' -- ' . $this->result->errorInfo()[2]);
        //     $this->logger->debug('Test Query: ' . $test_query);
        //     throw new DatabaseQueryException($this->result->errorInfo()[2]);
        // }
        return $this;
    }

    /**
     * Returns the current element in the result set as an array
     */
    public function current(): array
    {
        return oci_fetch_assoc($this->result);
    }

    /**
     * Return the current element and advances the pointer to the next element. This should be used for iterating over
     * the result set and should not be mixed with use of current().
     *
     * @return array|bool Returns the current result row.
     */
    public function fetch()
    {
        return oci_fetch_assoc($this->result);
    }

    /**
     * Checks if current position is valid
     *
     * @return boolean The return value will be casted to boolean and then evaluated.
     *       Returns true on success or false on failure.
     */
    public function valid(): bool
    {
        if (false === $this->fetch()) {
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
        $return = [];
        oci_fetch_all($this->result, $return, 0, -1, OCI_FETCHSTATEMENT_BY_ROW + OCI_NUM);
        return $return;
    }

    /**
     * @inheritDoc
     */
    public function numRows(): int
    {
        // This is not supported
        return null;
    }
}