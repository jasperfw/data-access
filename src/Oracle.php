<?php /** @noinspection PhpComposerExtensionStubsInspection */

namespace JasperFW\DataAccess;

use Exception;
use JasperFW\DataAccess\Exception\DatabaseConnectionException;
use JasperFW\DataAccess\Exception\DatabaseQueryException;
use JasperFW\DataAccess\Exception\TransactionErrorException;
use JasperFW\DataAccess\Exception\TransactionsNotSupportedException;
use JasperFW\DataAccess\ResultSet\ResultSet;
use JasperFW\DataAccess\ResultSet\ResultSetOracle;
use PDOStatement;
use Psr\Log\LoggerInterface;

/**
 * Class Oracle
 *
 * Connection and Query Manager for Oracle database connections. Uses OCI8 to connect to the database. Does not use PDO.
 *
 * @package JasperFW\DataAccess
 */
class Oracle extends DAO
{
    private $dbconn;
    private $stmt;
    private $querySucceeded = false;

    public function __construct(array $configuration, ?LoggerInterface $logger = null)
    {
        parent::__construct($configuration, $logger);
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
        $this->logger->debug('Connecting to Oracle database engine.');
        $this->dbconn = oci_connect(
            $this->configuration['username'],
            $this->configuration['password'],
            $this->configuration['server']
        );
        if (!$this->dbconn) {
            $m = oci_error();
            $errorMessage = 'Oracle Connection to <strong>' . $this->configuration['server'] . '</strong> failed!';
            $this->logger->warning($errorMessage, [$m['message']->getMessage()]);
            throw new DatabaseConnectionException($m['message']);
        }
        $this->is_connected = true;
        $this->logger->debug('Connected to database engine.');
    }

    /**
     * Closes the PDO statement and database connection
     */
    public function disconnect(): void
    {
        if ($this->is_connected) {
            oci_rollback($this->dbconn); // Rollback any uncommitted transactions
        }
        oci_close($this->dbconn);
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
        $this->inTransaction = true;
        return true;
    }

    /**
     * Rollback the transaction.
     *
     * @return bool
     * @throws TransactionErrorException if there is no active transaction
     */
    public function rollbackTransaction(): bool
    {
        return oci_rollback($this->dbconn);
    }

    /**
     * Commit the transaction.
     *
     * @return bool
     * @throws TransactionErrorException
     */
    public function commitTransaction(): bool
    {
        $this->inTransaction = false;
        return oci_commit($this->dbconn);
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
        $this->querySucceeded = false;
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
            $this->querySucceeded = true;
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
    public function getStatement(string $query_string): ?ResultSet
    {
        // Connect to the database
        if (!$this->is_connected) {
            $this->connect();
        }
        $stmt = oci_parse($this->dbconn, $query_string);
        if (false === $stmt) {
            // The statement could not be prepared, return an appropriate exception
            $m = oci_error();
            $errorMessage = 'The query could not be prepared.';
            $this->logger->warning($errorMessage, [$m['message']->getMessage()]);
            throw new DatabaseQueryException('The query could not be prepared. ' . $m['message']);
        }
        return new ResultSetOracle($stmt, $this, $this->logger);
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
        return 0;
    }
}