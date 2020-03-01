# Jasper FW - Data Access

The Data Access Library provides wrappers for interacting with MySQL, SQL Server, SQLite and more coming soon.

The included database connection wrappers mostly wrap PDO drivers. However, some drivers wrap alternative drivers (such
as odbi for Oracle) where PDO support is not available or complete.

In addition to database connectivity, this library provides a set of ResultSet objects that wrap PDOStatment objects,
arrays, or other data holders. These ResultSet objects can be used as prepared statements and storage objects for the
results of queries against a wide variety of data sources with a simple, uniform interface.

## Features

- AIntegrate with popular database engines (MySQL and MS Server currently, partial DB2 support, more coming soon.)
- Simple Prepared Statement / result containers

# Instructions

## Installation
Install using composer `composer require "jasperfw/data-access"`

## Basic Usage
### Establish a database connection
1. Create a configuration array containing the database connection attributes
```php
$config = [
    'server' => 'mysql://localhost:port', // This is your DSN string for most database types
    'username' => 'bob',
    'password' => 'myUniquePassword',
];
```
2. Instantiate a database connection object, passing in the $configuration array, and optionally a PSR-3 compliant
logger, such as Monolog 
```php
$dbc = new MySQL($config, $logger);
```
### Query the database
1. Quickly query the database, and output the results as an array. The `query()` function accepts two parameters, the
query string, and an options array, which can contain a 'params' element for a parameterized query:
```php
$queryString = "SELECT * FROM users WHERE username = :username";
$myArray = $dbc->query($queryString, ['params' => [':username' => 'bob']])->toArray();
```
2. Use parameterized queries and Result Sets
```php
$queryString = "SELECT * FROM order_lines WHERE productID = :pid";
$stmt = $dbc->getStatement($queryString);
$stmt->execute([':pid' => '12345']);
if ($stmt->querySucceeded()) {
    $resultArray = $stmt->toArray();
}
$stmt->execute([':pid' => '54321']);
if ($stmt->querySucceeded()) {
    $resultArray2 = $stmt->toArray();
}
```