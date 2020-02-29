<?php /** @noinspection PhpComposerExtensionStubsInspection */

namespace JasperFW\DataAccess;

use JasperFW\DataAccess\Exception\DatabaseConnectionException;
use JasperFW\DataAccess\Exception\DatabaseQueryException;
use JasperFW\DataAccess\Exception\TransactionsNotSupportedException;
use JasperFW\DataAccess\ResultSet\ResultSet;
use JasperFW\DataAccess\ResultSet\ResultSetArray;
use PDOStatement;
use Psr\Log\LoggerInterface;

/**
 * Class LDAP
 *
 * Data access class for LDAP queries. In addition to normal data lookups, this class also features a full login
 * process to allow for user authentication.
 *
 * @package      JasperFW\DataAccess
 */
class LDAP extends DAO
{
    /** @var bool True if the last query succeeded */
    protected $querySucceeded = false;
    /** @var resource The ldap connection handle */
    protected $handle;
    /** @var ResultSet The result */
    protected $result;

    /**
     * Generates the object. This does not connect to the server - that should be done only by the query function so
     * that connections are only loaded if they are being used.
     *
     * @param array           $config Configuration settings for the connection this object represents
     * @param LoggerInterface $logger The log manager
     *
     * @throws DatabaseConnectionException
     */
    public function __construct(array $config, LoggerInterface $logger = null)
    {
        parent::__construct($config, $logger);
    }

    /**
     * Establish a connection to the database server.
     *
     * @return mixed
     * @throws DatabaseConnectionException
     */
    public function connect(): void
    {
        $this->logger->info('Connecting to LDAP');
        // Connect to the LDAP server
        $handle = ldap_connect($this->configuration['server'], $this->configuration['port']);

        if (is_resource($handle)) {
            ldap_set_option($handle, LDAP_OPT_PROTOCOL_VERSION, 3);
            ldap_set_option($handle, LDAP_OPT_REFERRALS, 0);
            $this->logger->info('Connected to LDAP');
            $this->handle = $handle;
            $this->logger->info('Connected to LDAP');
            $this->isConnected = true;
        } else {
            throw new DatabaseConnectionException('Unable to connect to LDAP');
        }
    }

    /**
     * Close the connection if it has been established.
     */
    public function disconnect(): void
    {
        $this->handle = false;
        $this->isConnected = false;
    }

    /**
     * Start a transaction.
     *
     * @return bool
     * @throws TransactionsNotSupportedException
     */
    public function beginTransaction(): bool
    {
        throw new TransactionsNotSupportedException("Transactions are not supported in LDAP connections");
    }

    /**
     * Rollback the transaction.
     *
     * @return bool
     * @throws TransactionsNotSupportedException
     */
    public function rollbackTransaction(): bool
    {
        throw new TransactionsNotSupportedException("Transactions are not supported in LDAP connections");
    }

    /**
     * Commit the transaction.
     *
     * @return bool
     * @throws TransactionsNotSupportedException
     */
    public function commitTransaction(): bool
    {
        throw new TransactionsNotSupportedException("Transactions are not supported in LDAP connections");
    }

    /**
     * Execute a query with the passed options. Typically the options array will include a params subarray to run the
     * query as a prepared statement.
     *
     * @param string $queryString
     * @param array  $params
     * @param array  $options
     *
     * @return DAO
     * @throws DatabaseConnectionException
     * @throws DatabaseQueryException
     * @noinspection PhpComposerExtensionStubsInspection
     */
    public function query(string $queryString, array $params = [], array $options = []): DAO
    {
        // Set success to failure
        $this->querySucceeded = false;

        // Check if a connection has been established
        if (!$this->isConnected) {
            $this->connect();
        }
        // Perform the query
        $attributes = isset($options['attributes']) ? $options['attributes'] : [];
        $result = ldap_search($this->handle, $this->configuration['base_dn'], $queryString, $attributes);
        //\Framework::i()->log->info('LDAP Query: ' . $queryString);
        if (!is_resource($result)) {
            $this->logger->info('LDAP search failed: ' . ldap_err2str(ldap_errno($this->handle)));
            throw new DatabaseQueryException('LDAP Search failed.');
        }
        $this->result = new ResultSetArray(ldap_get_entries($this->handle, $result), $this, $this->logger);
        // Update the success status
        $this->querySucceeded = true;
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
    public function toArray(): array
    {
        return $this->result->toArray();
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
     * This is not supported by LDAP
     */
    public function lastInsertId(): int
    {
        return null;
    }

    /**
     * LDAP doesn't support pagination.
     *
     * @param int $page      The page in the result set to request
     * @param int $page_size The number of results in a page
     *
     * @return string
     */
    public function generatePagination(int $page = null, int $page_size = null): string
    {
        return '';
    }

    /**
     * LDAP doesn't support sorting.
     *
     * @param $column
     * @param $prepend
     *
     * @return string
     */
    public function generateSort(array $column, ?string $prepend = null): string
    {
        return '';
    }

    /**
     * Returns the handle for use in LDAP connections
     *
     * @return resource
     */
    public function getHandle()
    {
        return $this->handle;
    }

    /**
     * Use the connection to authenticate a user, the typical use case of this type of connection. Note that this method
     * will sanitize the username password and domain for use with LDAP. These values should not be sanitized by the
     * calling method.
     *
     * @param string $domain
     * @param string $username
     * @param string $password
     *
     * @return bool
     * @throws DatabaseConnectionException If connection with the autnentication server fails
     */
    public function authenticateUser(string $domain, string $username, string $password): bool
    {
        if (!$this->isConnected) {
            $this->connect();
        }
        $this->logger->debug('Attempting to authenticate ' . $username . '@' . $domain);
        $username = ldap_escape($username);
        $password = ldap_escape($password);
        $domain = ldap_escape($domain);
        $success = ldap_bind($this->handle, $username . '@' . $domain, $password);
        if ($success != true) {
            $this->logger->warning('Failed authenticating ' . $username . '@' . $domain);
        }
        return $success;
    }

    /**
     * The binding sends the username, domain and password to the remote server. In order to log in, the user's
     * information is sent instead of the default account information. After a login, the default binding must be
     * reset for additional queries.
     *
     * @throws DatabaseConnectionException
     * @noinspection PhpComposerExtensionStubsInspection
     */
    public function bindToServer(): void
    {
        $success = @ldap_bind(
            $this->handle,
            $this->configuration['username'] . '@' . $this->configuration['domain'],
            $this->configuration['password']
        );
        if (!$success) {
            $this->logger->error('LDAP failed: ' . ldap_err2str(ldap_errno($this->handle)));
            if (ldap_errno($this->handle) == '49') {
                throw new DatabaseConnectionException('Unable to connect to LDAP server.');
            } else {
                throw new DatabaseConnectionException('A problem occurred when binding to the LDAP server.');
            }
        }
    }

    /**
     * Use this function to free up resources and perform any required cleanup when the dao is released.
     */
    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * Returns a statement object representing a prepared statement for the database. Not supported for LDAP
     *
     * @param string $queryString The query
     *
     * @return ResultSet
     */
    public function getStatement(string $queryString): ?ResultSet
    {
        return null;
    }

    /**
     * Checks the configuration array for required elements. By default checks the server, username and password
     * elements are set. Override to check for additional elements.
     *
     * @param array $config
     *
     * @return bool
     * @throws DatabaseConnectionException
     */
    protected function validateConfiguration(array $config): bool
    {
        if (!is_array($config)) {
            throw new DatabaseConnectionException('Could not load LDAP configuration settings.');
        }
        $config_keys = array_keys($config);
        $missing = array_diff(['server', 'port'], $config_keys);
        if (count($missing) > 0) {
            throw new DatabaseConnectionException('LDAP configuration missing ' . implode(', ', $missing));
        }
        return true;
    }
}