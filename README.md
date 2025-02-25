Gearman
=======

[![Build Status](https://img.shields.io/travis/websightnl/gearman/master.svg?style=flat)](https://travis-ci.org/websightnl/gearman)
[![Latest Stable Version](http://img.shields.io/packagist/v/websightnl/gearman.svg?style=flat)](https://packagist.org/packages/websightnl/gearman)
[![Total Downloads](https://img.shields.io/packagist/dt/websightnl/gearman.svg?style=flat)](https://packagist.org/packages/websightnl/gearman)
[![License](https://img.shields.io/packagist/l/websightnl/gearman.svg?style=flat)](https://packagist.org/packages/websightnl/gearman)

PHP library for dispatching, handling and managing Gearman Workers

_**Todo:** Add support for tasks, only jobs are handled right now._<br>
_**Todo:** Tests are working but could cover more._

## Table Of Content

1. [Requirements](#requirements)
2. [Installation](#installation)
3. [Config](#config)
4. [Bootstrap](#bootstrap)
5. [Job example](#job-example)
6. [Dispatcher usage](#dispatcher-usage)
7. [Start workers daemon](#start-workers-daemon)
8. [Usage with Supervisor](#usage-with-supervisor)
9. [PHP 8.4 Compatibility](#php-8-4-compatibility)

<a name="requirements"></a>
## Requirements

This library uses PHP 5.4+, [PECL Gearman](http://php.net/manual/en/book.gearman.php) and 
[Gearman 1.0+](http://gearman.org).

<a name="installation"></a>
## Installation

It is recommended that you install the Gearman library [through composer](http://getcomposer.org/). To do so, add the following lines to your ``composer.json`` file.

```json
{
    "require": {
       "websightnl/gearman": "~1.0"
    }
}
```

<a name="config"></a>
## Config

The library uses a Config class to share configuration between classes.

#### Example

```php
use Sinergi\Gearman\Config;

$config = (new Config())
    ->addServer('127.0.0.1', 4730)
    ->setUser('apache');
```

#### Example using array

Alternatively, you can setup the config with an array.

```php
use Sinergi\Gearman\Config;

$config = new Config([
    'servers' => ['127.0.0.1:4730', '127.0.0.1:4731'],
    'user' => 'apache'
]);
```

<a name="config-parameters"></a>
#### Parameters

 * string __server__<br>
   The Gearman Server (E.G. 127.0.0.1:4730).
   
 * array __servers__<br>
   Pool of Gearman Servers.

 * string __bootstrap__<br>
   Path to the bootstrap file.
   
 * string __class__<br>
   Fully qualified name of the bootstrap class, the class needs to implement the `Sinergi\Gearman\BootstrapInterface` interface.
   
 * array __env_variables__<br>
   Environment variables you want to send to your bootstrap.
   
 * string __user__<br>
   The user under which the Gearman Workers will run (E.G. apache).
   
 * bool __auto_update__<br> 
   Use for __*development only*__, automatically updates workers before doing a job or task.

 * string __pidFilename__<br> 
   Change the filename of the created PID file (defaults to gearmanhandler.pid).  The file is always created in the system temp path.
   
 * string __lockFilename__<br> 
   Change the filename of the created lock file (defaults to gearmanhandler.lock). The file is always created in the system temp path.
   
 * int __loopTimeout__<br> 
   Change the time (in milliseconds) between pinging the Gearman server. Defaults to the low value of 10 milliseconds, for legacy reasons. **Change this value if you experience high load on your Gearman server!**
   
<a name="bootstrap"></a>
## Bootstrap

File `/path/to/your/bootstrap.php`

```php
use Sinergi\Gearman\BootstrapInterface;
use Sinergi\Gearman\Application;

class MyBootstrap implements BootstrapInterface
{
    public function run(Application $application)
    {
        $application->add(new JobExample());
    }
}
```

<a name="job-example"></a>
## Job example

```php
use Sinergi\Gearman\JobInterface;
use GearmanJob;

class JobExample implements JobInterface
{
    public function getName()
    {
        return 'JobExample';
    }

    public function execute(GearmanJob $job)
    {
        // Do something
    }
}
```

<a name="dispatcher-usage"></a>
## Dispatcher usage

To send tasks and jobs to the Workers, use the Dispatcher like this:

```php
use Sinergi\Gearman\Dispatcher;

$dispatcher = new Dispatcher($config);
$dispatcher->execute('JobExample', ['data' => 'value']);
```

<a name="start-workers-daemon"></a>
## Start workers daemon

Starts the Workers as a daemon. You can use something like supervisor to make sure the Workers are always running.
You can use the same parameters as in the [config](#config-parameters).

#### Single server

```shell
php vendor/bin/gearman start --bootstrap="/path/to/your/bootstrap.php" --class="MyBootstrap" --server="127.0.0.1:4730"
```

#### Multiple servers

```shell
php vendor/bin/gearman start --bootstrap="/path/to/your/bootstrap.php" --class="MyBootstrap" --servers="127.0.0.1:4730,127.0.0.1:4731"
```

#### List of commands

 * `start`
 * `stop`
 * `restart`


<a name="usage-with-supervisor"></a>
## Usage with Supervisor

This is an example of a Supervisor configuration. Add it to your Supervisor configuration file (E.G. /etc/supervisord.conf).

```
[program:mygearman]
command=php /path/to/vendor/bin/gearman start --daemon=false
process_name=%(program_name)s-procnum-%(process_num)s
numprocs=12
autostart=true
autorestart=true
```

<a name="php-8-4-compatibility"></a>
## PHP 8.4 Compatibility

This library has been updated to support PHP 8.4+ with the following improvements:

- Strong type declarations throughout the codebase
- Modern property declarations
- Updated serialization methods
- Improved error handling
- Modern dependency versions
- Strict typing enabled
- Updated PHPUnit testing suite

To use with PHP 8.4+, ensure you have:

- PHP 8.4 or higher
- Latest PECL Gearman extension
- Gearman 1.0+ server
