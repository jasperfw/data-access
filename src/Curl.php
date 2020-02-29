<?php /** @noinspection PhpComposerExtensionStubsInspection */

namespace JasperFW\DataAccess;

/**
 * Class Curl
 *
 * This class is a wrapper for CURL functionality
 *
 * @package JasperFW\\DataAccess
 */
class Curl
{
    /** @var resource */
    private $ch;

    /**
     * Curl constructor.
     *
     * @param string|null $url
     */
    public function __construct(string $url = null)
    {
        $this->ch = curl_init($url);
    }

    /**
     * Set an option for the curl transfer
     *
     * @param string $name  The name of the option to set
     * @param mixed  $value The value to set
     *
     * @return bool
     */
    public function setopt(string $name, $value): bool
    {
        return curl_setopt($this->ch, $name, $value);
    }

    /**
     * Execute the CURL request
     *
     * @return mixed
     */
    public function exec()
    {
        return curl_exec($this->ch);
    }
}