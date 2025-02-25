<?php
declare(strict_types=1);

namespace Sinergi\Gearman;

use GearmanException;
use GearmanWorker;
use Psr\Log\LoggerInterface;
use Sinergi\Gearman\Exception\ServerConnectionException;

class Worker
{
    /**
     * @var GearmanWorker
     */
    private ?GearmanWorker $worker = null;

    /**
     * @var Config
     */
    private Config $config;

    /**
     * @var LoggerInterface
     */
    private ?LoggerInterface $logger;

    /**
     * @param Config $config
     * @param null|LoggerInterface $logger
     * @throws ServerConnectionException
     */
    public function __construct(
        Config $config,
        ?LoggerInterface $logger = null
    ) {
        $this->config = $config;
        $this->logger = $logger;
    }

    public function resetWorker(): void
    {
        if ($this->worker instanceof GearmanWorker) {
            $this->worker->unregisterAll();
        }
        $this->worker = null;
        $this->createWorker();
    }

    /**
     * @throws ServerConnectionException
     */
    private function createWorker(): void
    {
        $this->worker = new GearmanWorker();
        $servers = $this->config->getServers();
        $exceptions = [];
        
        foreach ($servers as $server) {
            try {
                $this->worker->addServer($server->getHost(), $server->getPort());
            } catch (GearmanException $e) {
                $message = sprintf(
                    'Unable to connect to Gearman Server %s:%s',
                    $server->getHost(),
                    $server->getPort()
                );
                
                if ($this->logger !== null) {
                    $this->logger->info($message);
                }
                
                $exceptions[] = $message;
            }
        }

        if (!empty($exceptions)) {
            throw new ServerConnectionException(implode(', ', $exceptions));
        }
    }

    /**
     * @return Config
     */
    public function getConfig()
    {
        return $this->config;
    }

    /**
     * @param Config $config
     * @return $this
     */
    public function setConfig(Config $config)
    {
        $this->config = $config;
        return $this;
    }

    /**
     * @return GearmanWorker
     */
    public function getWorker()
    {
        if (null === $this->worker) {
            $this->createWorker();
        }
        return $this->worker;
    }

    /**
     * @param GearmanWorker $worker
     * @return $this
     */
    public function setWorker(GearmanWorker $worker)
    {
        $this->worker = $worker;
        return $this;
    }

    /**
     * @return LoggerInterface
     */
    public function getLogger()
    {
        return $this->logger;
    }

    /**
     * @param null|LoggerInterface $logger
     * @return $this
     */
    public function setLogger(LoggerInterface $logger = null)
    {
        $this->logger = $logger;
        return $this;
    }
}
