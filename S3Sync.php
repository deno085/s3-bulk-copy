<?php

use Aws\S3\S3Client;
use Aws\S3\Enum\Permission;
use Aws\S3\Enum\Group;
use Aws\S3\Model\AcpBuilder;
use Aws\Common\Enum\Size;
use Aws\Common\Exception\MultipartUploadException;
use Aws\S3\Model\MultipartUpload\UploadBuilder;
use Guzzle\Common;
use Aws\S3\Sync;

/**
 * Perform batch S3 Sync operations
 */
class S3Sync
{
    /**
     * Bucket Name
     * @var string 
     */
    public $bucketName;

    /**
     * AWS API Key
     * @var string 
     */
    public $awsApiKey;
    
    /**
     * AWS API Secret
     * @var string 
     */
    public $awsSecretKey;
    
    /**
     * Default region (optional)
     * @var string 
     */
    public $awsRegion;
    
    /**
     * AWS session token
     * @var string 
     */
    public $awsToken;

    /**
     * Number of concurrent operations
     * @var Integer 
     */
    public $concurrency;

    private $s3Client;
    private $maxRetries;
    private $concurrentUploads;
    private $concurrentDownloads;

    protected function debug($str)
    {
        echo microtime().':'. $str."\n";
    }

    protected function error($str)
    {
        echo microtime()." ERROR: $str\n";
    }

    /**
     *
     * @param int|null $concurrency
     * @param int|null $retries
     */
    public function __construct($concurrentUploads = 10, $concurrentDownloads = 10,  $retries = 3)
    {
        $this->concurrentUploads = $concurrentUploads;
        $this->concurrentDownloads = $concurrentDownloads;
        $this->maxRetries = $retries;
    }

    protected function awsConnect()
    {
        if(is_object($this->s3Client))
        {
            return $this->s3Client;
        }
        if(!strlen($this->bucketName))
        {
            throw new \Exception('Missing Bucket Name');
        }
        $s3credentials = array(
            'key'    => $this->awsApiKey,
            'secret' => $this->awsSecretKey,
            'region' => $this->awsRegion
        );

        $token =  $this->awsToken;
        if(strlen($token))
        {
            $s3credentials['token'] = $token;
        }
        print_r($s3credentials);
        $this->s3Client = s3Client::factory($s3credentials);
        return $this->s3Client;
    }

    protected function verifyBucketExists(S3Client $s3Client)
    {
        if(!strlen($this->bucketName))
        {
            throw new Exception("[S3Sync::verifyBucketExists] Unable to verify S3 bucket: bucket name not set");
        }
        if(!$s3Client->doesBucketExist($this->bucketName))
        {
            $result = $s3Client->createBucket(array(
                'ACL'=>'private',
                'Bucket'=>$this->bucketName
            ));
            $s3Client->waitUntilBucketExists(array('Bucket' => $this->bucketName));
            if(!$s3Client->doesBucketExist($this->bucketName))
            {
                throw new Exception('[S3Sync::verifyBucketExists] Unable to create S3 bucket '.$this->bucketName);
            }
        }

        return true;
    }

    /**
     * Copy a directory from local space to S3
     *
     * @param string $remotePath
     * @param string $localPath
     * @param ProgressUpdater $progress
     * @return array
     */
    public function getRemoteDir($remotePath, $localPath, $progress=null)
    {
        if(substr($remotePath,-1)=='/')
        {
            $remotePath = substr($remotePath,0,-1);
        }
        $fileData = array();
        $fileData['exit_code'] = null;
        $s3Client = $this->awsConnect();
        if($this->verifyBucketExists($s3Client))
        {
            if(!is_dir($localPath))
            {
                mkdir($localPath, 0777, true);
            }
            $downloader = \Aws\S3\Sync\DownloadSyncBuilder::getInstance()
                ->setBucket($this->bucketName)
                ->setClient($s3Client)
                ->setConcurrency($this->concurrentDownloads)
                ->setBaseDir($remotePath)
                ->force(true)
                ->setKeyPrefix($remotePath);
            $downloader->setDirectory($localPath);
            $downloadSync = $downloader->build();

            //add callback to progress updater and logging
            $downloadSync->getEventDispatcher()->addListener(Sync\DownloadSync::AFTER_TRANSFER, function (Guzzle\Common\Event $e) use ($progress, $fileData) {
                if(is_object($progress))
                {
                    $progress->incrementStep();
                }
                $c = $e['command'];
                $this->debug('[S3Sync::getRemoteDir] Copied '.$c->get('Key').' => '.$c->get('SaveAs'));
                $fileData[$c->get('Key')] = $c->get('SaveAs');
            });

            $downloadSync->transfer();
            $fileData['exit_code'] = 0;
        }
        else
        {
            $fileData['exit_code'] = 1;
        }
        return $fileData;
    }

    /**
     * Copy an S3 directory to local space
     *
     * @param string $localPath
     * @param string $remotePath
     * @param ProgressUpdater $progress
     * @param int $retries
     * @return array
     */
    public function putRemoteDir($localPath, $remotePath, $progress=null, $retries=0)
    {
        $result = null;     
        
        $s3Client = $this->awsConnect();

        if($this->verifyBucketExists($s3Client))
        {
            clearstatcache(true);
            $uploader = Aws\S3\Sync\UploadSyncBuilder::getInstance()
                ->setBucket($this->bucketName)
                ->setConcurrency($this->concurrentUploads)
                ->setBaseDir($localPath)
                ->setKeyPrefix($remotePath)
                ->setMultipartUploadSize(7168000)
                ->force(true)
                ->setClient($s3Client);
            if($retries==0)
            {
                $uploader->force(true);
            }
            $uploader->uploadFromDirectory($localPath);
            $uploadSync = $uploader->build();

            //add listener for progress updater and logging
            $uploadSync->getEventDispatcher()->addListener(Sync\UploadSync::AFTER_TRANSFER, function (Guzzle\Common\Event $e) use ($progress) {
                if(is_object($progress))
                {
                    $progress->incrementStep();
                }
            });
            try
            {
                $uploadSync->transfer();
                $result = true;
            }
            catch (MultipartUploadException $error)
            {
                $this->debug('[S3Sync::putRemoteDir] Resuming upload of '.$localPath.': '.$error->getMessage());
                $uploadSync->resumeFrom($error->getState());
            }
            catch (\Aws\Common\Exception\TransferException $error)
            {
                foreach ($error->getFailedCommands() as $failedCommand) {
                    $this->error('[S3Sync::putRemoteDir] '.$error->getExceptionForFailedCommand($failedCommand)->getMessage());
                }
                $retries++;
                if($retries < $this->maxRetries)
                {
                    unset($uploadSync);
                    unset($uploader);
                    $this->debug('[S3Sync::putRemoteDir] retrying attempt #'.$retries.' for '.$localPath);
                    return $this->putRemoteDir($localPath, $remotePath, $progress, $retries);
                }
                $result = false;
            }
            catch(\Guzzle\Service\Exception\CommandTransferException $error)
            {
                foreach($error->getFailedCommands() as $failedCommand)
                {
                    $this->error('[S3Sync::putRemoteDir] '.$error->getExceptionForFailedCommand($failedCommand)->getMessage());
                }
                $retries++;
                if($retries < $this->maxRetries)
                {
                    unset($uploadSync);
                    unset($uploader);
                    $this->debug('[S3Sync::putRemoteDir] retrying attempt #'.$retries.' for '.$localPath);
                    return $this->putRemoteDir($localPath, $remotePath, $progress, $retries);
                }
                $result = false;
            }
            catch (\Exception $e)
            {
                $this->error('S3Sync::putRemoteDir] Error: '.$e->getMessage());
            }
        }
        else
        {
            $result = false;
        }
        return $fileData;
    }

    /**
     * Perform a remote -> remote batch copy operation
     * @param array $srcFiles
     * @param ProgressUpdater $progress
     * @return bool
     */
    public function batchCopy($srcFiles, $concurrency, $progress=null, $retries=0)
    {
        $s3Client = $this->awsConnect();

        if(count($srcFiles)>0)
        {
            $batchCount = floor(count($srcFiles) / $concurrency);
            if(count($srcFiles) / $concurrency % $concurrency != 0)
            {
                $batchCount++;
            }
            $failed = array();
            $batchNum = 0;
            $failedFiles = array();
            $batch = array();
            $this->debug('[S3Sync::batchCopy] Starting copy of '.count($srcFiles)." files");

            foreach($srcFiles as $srcFile => $destFile)
            {
                if(strpos($destFile, '::')!==false)
                {
                    $parts = explode('::', $destFile);
                    $targetBucket = $parts[0];
                    $dest = $parts[1];
                }
                else
                {
                    $targetBucket = $this->bucketName;
                    $dest = $destFile;
                }
                $this->debug('[S3Sync::batchCopy] Preparing command to copy '. $srcFile.' to '.$targetBucket.'/'.$dest);
                $batch[] = $s3Client->getCommand('CopyObject', array(
                    'Bucket'     => $targetBucket,
                    'Key'        => $dest,
                    'CopySource' => $srcFile,
                ));

                if(count($batch) > $concurrency)
                {
                    $this->debug('[S3Sync::batchCopy] Copy file batch '.($batchNum+1).' of '.$batchCount.' batches');
                    try
                    {
                        $s3Client->execute($batch);
                        if(!is_null($progress))
                        {
                            $progress->incrementStep(count($batch));
                        }
                        $this->debug('[S3Sync::batchCopy] Copy batch Complete '.($batchNum+1).' of '.$batchCount.' batches');
                    }
                    catch (\Guzzle\Service\Exception\CommandTransferException $e)
                    {
                        $failed = array_merge($failed, $e->getFailedCommands());
                        foreach($failed as $failedCommand)
                        {
                            $this->error('[S3Sync::copyBatch] '.$e->getExceptionForFailedCommand($failedCommand)->getMessage());
                            $failedFiles[$failedCommand->get('CopySource')] = $failedCommand->get('Key');
                        }
                    }
                    $batchNum++;
                    $batch = array();

                }
            }

            if(count($batch) > 0)
            {
                $this->debug('[S3Sync::batchCopy] Copy file batch '.($batchNum+1).' of '.$batchCount.' batches');
                try
                {
                    $s3Client->execute($batch);
                    if(!is_null($progress))
                    {
                        $progress->incrementStep(count($batch));
                    }
                    $this->debug('[S3Sync::batchCopy] Copy batch Complete '.($batchNum+1).' of '.$batchCount.' batches');
                }
                catch (\Guzzle\Service\Exception\CommandTransferException $e)
                {
                    $failed = array_merge($failed, $e->getFailedCommands());
                    foreach($failed as $failedCommand)
                    {
                        $this->error('[S3Sync::copyBatch] '.$e->getExceptionForFailedCommand($failedCommand)->getMessage());
                        $failedFiles[$failedCommand->get('CopySource')] = $failedCommand->get('Key');
                    }
                }
            }

            $numSuccess = count($srcFiles) - count($failedFiles);
            if(count($failedFiles)>0)
            {
                $this->debug('[S3Sync::batchCopy] Completed copy of '.$numSuccess.' out of '.count($srcFiles)." files");
                $this->debug("[S3Sync::batchCopy] failed files:\n" . implode("\n", $failedFiles));
                if($retries<2)
                {
                    $retryBatch = array();
                    foreach($failedFiles as $copySource=>$destKey)
                    {
                        $copySource = substr($copySource, strlen($this->bucketName)+1);
                        $retryBatch[$copySource] = $destKey;
                    }
                    $this->batchCopy($retryBatch, $concurrency, $progress, $retries+1);
                }
                else
                {
                    throw new Exception("[S3Sync::copyBatch] Maximum retries attempting to copy files. \n".implode("\n", $failedFiles));
                }
            }
            $this->debug('[S3Sync::batchCopy] Completed copy of '.count($srcFiles)." files");
            return true;
        }
        return false;
    }

    public function getFileListing($remotePath)
    {
        $files = array();
        $remotePath = str_replace('*', '', $remotePath); //turn path/* to path/
        $s3Client = $this->awsConnect();

        $iterator = $s3Client->getIterator('ListObjects', array(
            'Bucket'    => $this->bucketName,
            'Prefix'    => $remotePath,
        ));
        foreach($iterator as $object)
        {
            $key = $object['Key'];
            if(substr($key,-1)!='/')
            {
                $files[] = $key;
            }
        }

        return $files;
    }
}