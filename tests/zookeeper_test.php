<?php
/**
 *
 *
 * User: php-cpm
 * Date: 2017-08-16 19:26
 */

require_once "../vendor/autoload.php";

echo "KafkaMeta\\ZooKeeper::BROKER_DETAIL_PATH is ".\KafkaMeta\ZooKeeper::BROKER_DETAIL_PATH. "\r\n";
echo "KafkaMeta\\ZooKeeper::BROKER_PATH is ".\KafkaMeta\ZooKeeper::BROKER_PATH. "\r\n";

$hostList ="192.168.203.131:2181";
$client = new \KafkaMeta\ZooKeeper($hostList);

print_r($client->listBrokers());
print_r($client->getBrokerDetail(0));
print_r($client->getTopicDetail('topic-name'));
print_r($client->getPartitionState('topic-name',0));