<?php
namespace Flowpack\JobQueue\Common\Command;

/*
 * This file is part of the Flowpack.JobQueue.Common package.
 *
 * (c) Contributors to the package
 *
 * This package is Open Source Software. For the full copyright and license
 * information, please view the LICENSE file which was distributed with this
 * source code.
 */

use TYPO3\Flow\Annotations as Flow;
use TYPO3\Flow\Cli\CommandController;
use Flowpack\JobQueue\Common\Exception as JobQueueException;
use Flowpack\JobQueue\Common\Job\JobManager;
use Flowpack\JobQueue\Common\Queue\QueueManager;

/**
 * Job command controller
 */
class JobCommandController extends CommandController
{
    /**
     * @Flow\Inject
     * @var JobManager
     */
    protected $jobManager;

    /**
     * @Flow\Inject
     * @var QueueManager
     */
    protected $queueManager;

    /**
     * Work on a queue and execute jobs
     *
     * @param string $queueName The name of the queue
     * @param integer $limit The max number of jobs that should execute before exiting.
     * @param integer $maxTime The max time that jobs should be execute before exiting.
     * @return void
     */
    public function workCommand($queueName, $limit = 0, $maxTime = 0)
    {
        $hasLimit = ($limit > 0);
        $hasMaxTime = ($maxTime > 0);
        $runInfiniteJobs = !$hasLimit && !$hasMaxTime;

        if ($hasMaxTime) {
            $endTime = new \DateTime(sprintf('now +%d seconds', $maxTime));
        }

        $jobsDone = 0;
        do {
            try {
                $jobsDone++;
                $timeout = null;
                if ($hasMaxTime) {
                    $timeout = $endTime->getTimestamp() - (new \DateTime())->getTimestamp();
                }
                $this->jobManager->waitAndExecute($queueName, $timeout);
            } catch (JobQueueException $exception) {
                $this->outputLine($exception->getMessage());
                if ($exception->getPrevious() instanceof \Exception) {
                    $this->outputLine($exception->getPrevious()->getMessage());
                }
            } catch (\Exception $exception) {
                $this->outputLine('Unexpected exception during job execution: %s', array($exception->getMessage()));
            }

        } while ($runInfiniteJobs
            ||
            (
                (($hasLimit && $jobsDone < $limit) || !$hasLimit)
                &&
                (($hasMaxTime && ((new \DateTime) < $endTime)) || !$hasMaxTime)
            )
        );
    }

    /**
     * List queued jobs
     *
     * @param string $queueName The name of the queue
     * @param integer $limit Number of jobs to list
     * @return void
     */
    public function listCommand($queueName, $limit = 1)
    {
        $jobs = $this->jobManager->peek($queueName, $limit);
        $totalCount = $this->queueManager->getQueue($queueName)->count();
        foreach ($jobs as $job) {
            $this->outputLine('<u>%s</u>', array($job->getLabel()));
        }

        if ($totalCount > count($jobs)) {
            $this->outputLine('(%d omitted) ...', array($totalCount - count($jobs)));
        }
        $this->outputLine('(<b>%d total</b>)', array($totalCount));
    }
}
