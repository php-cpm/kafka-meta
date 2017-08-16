<?php

/**
 *
 * +------------------------------------------------------------------------------
 *     Metadata about the kafka cluster
 * +------------------------------------------------------------------------------
 *
 * I read info from: https://phabricator.wikimedia.org/T106256
 * I think I must master all the kafka protocols before I do more
 *
 * @package KafkaMeta
 * @version 0.1.0
 * @copyright Copyleft
 * @author ebernhardson@wikimedia.org
 * @editor php-cpm
 */

namespace KafkaMeta;


interface ClusterMetaData
{
    /**
     * get broker list from kafka metadata
     *
     * @access public
     * @return array
     */
    public function listBrokers();

    /**
     * @param string $topicName
     * @param integer $partitionId
     * @access public
     * @return array
     */
    public function getPartitionState($topicName, $partitionId = 0);

    /**
     * @param string $topicName
     * @access public
     * @return array
     */
    public function getTopicDetail($topicName);

    /**
     * @return null
     */
    public function refreshMetadata();
}
