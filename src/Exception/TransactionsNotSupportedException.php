<?php

namespace JasperFW\DataAccess\Exception;

/**
 * Class TransactionsNotSupportedException
 *
 * Exception thrown if a transaction is attempted but the database engine does not support transactions
 *
 * @package JasperFW\DataAccess\Exception
 */
class TransactionsNotSupportedException extends DatabaseException
{

}