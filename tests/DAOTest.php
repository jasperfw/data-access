<?php

namespace JasperFW\DataAccessTest;

use JasperFW\DataAccess\DAO;
use PHPUnit\Framework\TestCase;

class DAOTest extends TestCase
{
    public function testEscapeColumnName()
    {
        $column_name = 'bob';
        $escaped_column_name = '`bob`';
        $this->assertEquals($escaped_column_name, DAO::escapeColumnName($column_name));
    }
}
