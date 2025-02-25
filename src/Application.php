<?php
namespace Sinergi\Gearman;

use Closure;
use Exception;
use GearmanJob;
use Psr\Log\LoggerInterface;
use React\EventLoop\Factory as Loop;
use React\EventLoop\LoopInterface;
use Serializable;
use Sinergi\Gearman\Exception\InvalidBootstrapClassException;

class Application implements Serializable
{
    /**
     * @var Config
     */
    private Config $config;

    /**
     * @var Process
     */
    private Process $process;

    /**
     * @var array
     */
    private array $callbacks = [];

    /**
     * @var StreamSelectLoop|LibEventLoop
     */
    private ?LoopInterface $loop = null;

    /**
     * @var bool|resource
     */
    private bool|resource $lock = false;

    /**
     * @var bool
     */
    private bool $kill = false;

    /**
     * @var Worker
     */
    private ?Worker $worker = null;

    /**
     * @var array
     */
    private array $jobs = [];

    /**
     * @var LoggerInterface
     */
    public ?LoggerInterface $logger = null;

    /**
     * @var bool
     */
    public bool $isAllowingJob = false;

    /**
     * @var bool
     */
    public bool $isBootstraped = false;

    /**
     * @var Application
     */
    private static ?self $instance = null;

    /**
     * gets the instance via lazy initialization (created on first usage)
     *
     * @return self
     */
    public static function getInstance(): self
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    /**
     * @param Config $config
     * @param StreamSelectLoop|LibEventLoop $loop
     * @param Process $process
     * @param LoggerInterface|null $logger
     */
    public function __construct(
        ?Config $config = null,
        ?Process $process = null,
        ?LoopInterface $loop = null,
        ?LoggerInterface $logger = null
    ) {
        self::$instance = $this;

        $this->config = $config ?? Config::getInstance();
        $this->logger = $logger;
        
        if ($process !== null) {
            $this->setProcess($process);
        }
        
        if ($loop !== null) {
            $this->setLoop($loop);
        }
    }

    public function __destruct()
    {
        if (is_resource($this->lock)) {
            if (null !== $this->logger) {
                $this->logger->info("Stopped GearmanWorker Server");
            }
            $this->getProcess()->release($this->lock);
        }
    }

    public function restart()
    {
        $serialized = serialize($this);
        $file = realpath(__DIR__ . "/../../bin/gearman_restart");

        $serializedFile = sys_get_temp_dir() . '/gearman_restart_' . uniqid();
        file_put_contents($serializedFile, $serialized);

        if ($file && is_executable($file)) {
            pcntl_exec($file, ['serialized' => $serializedFile]);
            exit;
        } elseif ($file) {
            $dir = dirname($file);
            $content = file_get_contents($dir . '/gearman_restart_template');
            $content = str_replace('%path', $dir . '/gearman_restart.php', $content);
            $newFile = sys_get_temp_dir() . '/gearman_restart_' . uniqid();
            file_put_contents($newFile, $content);
            chmod($newFile, 0755);
            pcntl_exec($newFile, ['serialized' => $serializedFile]);
            unlink($newFile);
            exit;
        }
    }

    public function bootstrap($restart = false)
    {
        if ($this->getConfig()->getEnvVariables()) {
            $this->addEnvVariables();
        }

        $bootstrap = $this->getConfig()->getBootstrap();
        if (is_file($bootstrap)) {
            require_once $bootstrap;
            if ($restart && null !== self::$instance && spl_object_hash($this) !== spl_object_hash(self::$instance)) {
                self::$instance->unserialize($this->serialize());
                self::$instance->run(false, true);
                $this->kill = true;
            }
        }

        $class = $this->getConfig()->getClass();
        if (!empty($class)) {
            $bootstrap = new $class();
            if (!$bootstrap instanceof BootstrapInterface) {
                throw new InvalidBootstrapClassException;
            }
            $bootstrap->run($this);
        }

        $this->isBootstraped = true;
    }

    /**
     * @param bool $fork
     * @param bool $restart
     * @throws InvalidBootstrapClassException
     */
    public function run($fork = true, $restart = false)
    {
        if (!$restart) {
            $this->bootstrap();
        }

        $this->runProcess($fork, $restart);
    }

    public function addEnvVariables()
    {
        foreach ($this->getConfig()->getEnvVariables() as $key => $variable) {
            $key = (string)$key;
            $variable = (string)$variable;
            $var = "{$key}={$variable}";
            putenv($var);
        }
    }

    /**
     * @param bool $fork
     * @param bool $restart
     * @throws Exception
     */
    public function runProcess($fork = true, $restart = false)
    {
        $pidFile = $this->getProcess()->getPidFile();
        $lockFile = $this->getProcess()->getLockFile();
        if (is_file($pidFile) && is_writable($pidFile)) {
            unlink($pidFile);
        }
        if (is_file($lockFile) && is_writable($lockFile)) {
            unlink($lockFile);
        }

        $this->changeUser();

        if ($fork) {
            $pid = pcntl_fork();
        }

        if (!$fork || (isset($pid) && $pid !== -1 && !$pid)) {
            $this->getProcess()->setPid(posix_getpid());

            if (isset($pid) && $pid !== -1 && !$pid) {
                $parentPid = posix_getppid();
                if ($parentPid) {
                    posix_kill(posix_getppid(), SIGUSR2);
                }
            }

            $this->lock = $this->getProcess()->lock();

            if (null !== $this->logger) {
                $this->logger->info("Started GearmanWorker Server");
            }

            $this->signalHandlers();
            $this->createLoop($restart);
        } elseif ($fork && isset($pid) && $pid) {
            $wait = true;

            pcntl_signal(SIGUSR2, function () use (&$wait) {
                $wait = false;
            });

            while ($wait) {
                pcntl_waitpid($pid, $status, WNOHANG);
                pcntl_signal_dispatch();
            }
        }
    }

    /**
     * @throws Exception
     */
    private function changeUser()
    {
        $user = $this->getConfig()->getUser();
        if ($user) {
            $user = posix_getpwnam($user);
            if (posix_geteuid() !== (int)$user['uid']) {
                posix_setgid($user['gid']);
                posix_setuid($user['uid']);
                if (posix_geteuid() !== (int)$user['uid']) {
                    $message = "Unable to change user to {$user['uid']}";
                    if (null !== $this->logger) {
                        $this->logger->error($message);
                    }
                    throw new Exception($message);
                }
            }
        }
    }

    /**
     * @return $this
     */
    private function signalHandlers()
    {
        $root = $this;
        pcntl_signal(SIGUSR1, function () use ($root) {
            $root->setKill(true);
        });
        return $this;
    }

    /**
     * @param bool $restart
     * @return $this
     */
    private function createLoop($restart = false)
    {
        $worker = $this->getWorker()->getWorker();
        $worker->setTimeout($this->getConfig()->getLoopTimeout());

        $callbacks = $this->getCallbacks();

        if (!$this->isBootstraped) {
            $this->bootstrap(true);
        }

        if ($this->kill) {
            return null;
        }

        $callbacksCount = count($callbacks);

        declare (ticks=10) {
            while ($worker->work() || $worker->returnCode() == GEARMAN_TIMEOUT) {
                if ($this->getKill()) {
                    break;
                }

                pcntl_signal_dispatch();

                if ($callbacksCount) {
                    foreach ($callbacks as $callback) {
                        $callback($this);
                    }
                }
            }
        }

        if (!$this->getKill() && $worker instanceof \GearmanWorker) {
            if (null !== $this->logger) {
                $this->logger->error("Worker error {$worker->error()}");
            }
        }

        return $this;
    }

    /**
     * @param JobInterface $job
     * @param GearmanJob $gearmanJob
     * @param Application $root
     * @return mixed
     */
    public function executeJob(JobInterface $job, GearmanJob $gearmanJob, self $root): mixed
    {
        if ($root->getConfig()->getAutoUpdate() && !$root->isAllowingJob) {
            $root->restart();
            return null;
        }
        
        $root->isAllowingJob = false;
        
        if ($root->logger !== null) {
            $root->logger->info("Executing job {$job->getName()}");
        }
        
        return $job->execute($gearmanJob);
    }

    /**
     * @return Worker
     */
    public function getWorker()
    {
        if (null === $this->worker) {
            $this->setWorker(new Worker($this->getConfig(), $this->getLogger()));
        }
        return $this->worker;
    }

    /**
     * @param Worker $worker
     * @return $this
     */
    public function setWorker(Worker $worker)
    {
        $this->worker = $worker;
        return $this;
    }

    /**
     * @param JobInterface $job
     * @return $this
     */
    public function add(JobInterface $job)
    {
        $worker = $this->getWorker()->getWorker();

        $this->jobs[] = $job;
        $root = $this;
        $worker->addFunction($job->getName(), function (\GearmanJob $gearmanJob) use ($root, $job) {
            $retval = $root->executeJob($job, $gearmanJob, $root);
            return serialize($retval);
        });
        return $this;
    }

    /**
     * @return array
     */
    public function getJobs()
    {
        return $this->jobs;
    }

    /**
     * @param Closure $callback
     * @return $this
     */
    public function addCallback(Closure $callback)
    {
        $this->callbacks[] = $callback;
        return $this;
    }

    /**
     * @return array
     */
    public function getCallbacks()
    {
        return $this->callbacks;
    }

    /**
     * @param StreamSelectLoop|LibEventLoop $loop
     * @return $this
     */
    public function setLoop($loop)
    {
        $this->loop = $loop;
        return $this;
    }

    /**
     * @return LibEventLoop|StreamSelectLoop
     */
    public function getLoop()
    {
        if (null === $this->loop) {
            $this->setLoop(Loop::create());
        }
        return $this->loop;
    }

    /**
     * @return bool
     */
    public function getKill()
    {
        return $this->kill;
    }

    /**
     * @param $kill
     * @return $this
     */
    public function setKill($kill)
    {
        $this->kill = $kill;
        return $this;
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
     * @return Config
     */
    public function getConfig()
    {
        if (null === $this->config) {
            $this->setConfig(new Config);
        }
        return $this->config;
    }

    /**
     * @param Process $process
     * @return $this
     */
    public function setProcess(Process $process)
    {
        $this->process = $process;
        return $this;
    }

    /**
     * @return Process
     */
    public function getProcess()
    {
        if (null === $this->process) {
            $this->setProcess(new Process($this->getConfig(), $this->getLogger()));
        }
        return $this->process;
    }

    /**
     * @return string
     */
    public function serialize(): string
    {
        return serialize($this->__serialize());
    }

    public function __serialize(): array
    {
        return [
            'config' => $this->config,
            'process' => $this->process,
            'callbacks' => $this->callbacks,
            'jobs' => $this->jobs,
            'isAllowingJob' => $this->isAllowingJob,
            'isBootstraped' => $this->isBootstraped
        ];
    }

    public function unserialize(string $data): void
    {
        $this->__unserialize(unserialize($data));
    }

    public function __unserialize(array $data): void
    {
        $this->config = $data['config'];
        $this->process = $data['process'];
        $this->callbacks = $data['callbacks'];
        $this->jobs = $data['jobs'];
        $this->isAllowingJob = $data['isAllowingJob'];
        $this->isBootstraped = $data['isBootstraped'];
    }
}
