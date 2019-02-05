<?php

/*
 * Copyright (c) 2012 The University of Queensland
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *
 * Written by David Gwynne <dlg@uq.edu.au> as part of the IT
 * Infrastructure Group in the Faculty of Engineering, Architecture
 * and Information Technology.
 */

namespace SimpleSAML\Module\riak\Store;

use Basho\Riak\Bucket;
use Basho\Riak\Command\Builder\DeleteObject;
use Basho\Riak\Command\Builder\FetchObject;
use Basho\Riak\Command\Builder\StoreObject;
use Basho\Riak\Location;
use Basho\Riak\Node;
use Basho\Riak as RiakClient;

use SimpleSAML\Configuration;
use SimpleSAML\Store;

class Riak extends Store
{
    /** @var \Basho\Riak\Riak */
    public $client;

    /** @var \Basho\Riak\Bucket */
    public $bucket;

    protected function __construct()
    {
        $config = Configuration::getConfig('module_riak.php');

        $host = $config->getString('host', 'localhost');
        $port = $config->getString('port', 8098);
        $bucket = $config->getString('bucket', 'simpleSAMLphp');

        $node1 = (new Node\Builder)
          ->atHost($host)
          ->onPort($port)
          ->build();

        $this->client = new RiakClient([$node1]);
        $this->bucket = new Bucket($bucket);
    }


    /**
     * Retrieve a value from the datastore.
     *
     * @param string $type  The datatype.
     * @param string $key   The key.
     * @return mixed|null   The value.
     */
    public function get($type, $key)
    {
        assert(is_string($type));
        assert(is_string($key));

        $location = new Location($type, $this->bucket);
        $response = (new FetchObject($this->client))
           ->atLocation($location)
           ->withDecodeAsAssociative()
           ->build()
           ->execute();

        if ($response->getObject() === null) {
            return null;
        }
        $data = $response->getObject()->getData();
        if (isset($data['expires']) && (intval($data['expires']) <= time())) {
            $this->delete($type, $key);
            return null;
        }

        return unserialize($data[$key]);
    }


    /**
     * Save a value to the datastore.
     *
     * @param string $type  The datatype.
     * @param string $key   The key.
     * @param mixed $value  The value.
     * @param int|null $expire  The expiration time (unix timestamp), or NULL if it never expires.
     * @return void
     */
    public function set($type, $key, $value, $expire = null)
    {
        assert(is_string($type));
        assert(is_string($key));
        assert($expire === null || (is_int($expire) && $expire > 2592000));

        $location = new Location($type, $this->bucket);
        $data = [$key => serialize($value)];

        if (!is_null($expire)) {
            $data['expires'] = intval($expire);
        }

        $storecmd = (new StoreObject($this->client))
          ->buildJsonObject($data)
          ->atLocation($location)
          ->build();

        $storecmd->execute();
    }


    /**
     * Delete a value from the datastore.
     *
     * @param string $type  The datatype.
     * @param string $key   The key.
     * @return void
     */
    public function delete($type, $key)
    {
        assert(is_string($type));
        assert(is_string($key));

        $location = new Location($type, $this->bucket);
        (new DeleteObject($this->client))->atLocation($location)->build()->execute();
    }
}
