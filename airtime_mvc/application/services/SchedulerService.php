<?php
class Application_Service_SchedulerService
{
    private $con;
    private $fileInfo = array(
            "id" => "",
            "cliplength" => "",
            "cuein" => "00:00:00",
            "cueout" => "00:00:00",
            "fadein" => "00:00:00",
            "fadeout" => "00:00:00",
            "sched_id" => null,
            "type" => 0 //default type of '0' to represent files. type '1' represents a webstream
        );

    private $epochNow;
    private $nowDT;
    private $scheduleLocations = array();
    private $ccShow;
    private $insertValues = array();
    private $excludeIds = array();

    // Schedule position within each show instance. We need to keep track of
    // this in case shows are linked so we know where to insert items across
    // linked shows
    private $position;

    private $crossfadeDuration;

    public function __construct()
    {
        $this->con = Propel::getConnection(CcSchedulePeer::DATABASE_NAME);

        //subtracting one because sometimes when we cancel a track, we set its end time
        //to epochNow and then send the new schedule to pypo. Sometimes the currently cancelled
        //track can still be included in the new schedule because it may have a few ms left to play.
        //subtracting 1 second from epochNow resolves this issue.
        $this->epochNow = microtime(true)-1;
        $this->nowDT = DateTime::createFromFormat("U.u", $this->epochNow, new DateTimeZone("UTC"));

        if ($this->nowDT === false) {
            // DateTime::createFromFormat does not support millisecond string formatting in PHP 5.3.2 (Ubuntu 10.04).
            // In PHP 5.3.3 (Ubuntu 10.10), this has been fixed.
            $this->nowDT = DateTime::createFromFormat("U", time(), new DateTimeZone("UTC"));
        }

        $this->crossfadeDuration = Application_Model_Preference::GetDefaultCrossfadeDuration();
    }

    /**
     * 
     * Applies the show start difference to any scheduled items
     * 
     * @param $instanceIds
     * @param $diff
     * @param $newStart
     */
    public static function updateScheduleStartTime($instanceIds, $diff=null, $newStart=null)
    {
        $con = Propel::getConnection();
        if (count($instanceIds) > 0) {
            $showIdList = implode(",", $instanceIds);

            if (is_null($diff)) {
                $ccSchedule = CcScheduleQuery::create()
                    ->filterByDbInstanceId($instanceIds, Criteria::IN)
                    ->orderByDbStarts()
                    ->limit(1)
                    ->findOne();

                if (!is_null($ccSchedule)) {
                    $scheduleStartsEpoch = strtotime($ccSchedule->getDbStarts());
                    $showStartsEpoch     = strtotime($newStart->format("Y-m-d H:i:s"));

                    $diff = $showStartsEpoch - $scheduleStartsEpoch;
                }
            }

            $ccSchedules = CcScheduleQuery::create()
                ->filterByDbInstanceId($instanceIds, Criteria::IN)
                ->find();

            $interval = new DateInterval("PT".abs($diff)."S");
            if ($diff < 0) {
                $interval->invert = 1;
            }
            foreach ($ccSchedules as $ccSchedule) {
                $start = new DateTime($ccSchedule->getDbStarts());
                $newStart = $start->add($interval);
                $end = new DateTime($ccSchedule->getDbEnds());
                $newEnd = $end->add($interval);
                $ccSchedule
                    ->setDbStarts($newStart->format("Y-m-d H:i:s"))
                    ->setDbEnds($newEnd->format("Y-m-d H:i:s"))
                    ->save();
            }
        }
    }

    /**
     * 
     * Removes any time gaps in shows
     * 
     * @param array $schedIds schedule ids to exclude
     */
    public function removeGaps($showId, $schedIds=null)
    {
        $ccShowInstances = CcShowInstancesQuery::create()->filterByDbShowId($showId)->find();

        foreach ($ccShowInstances as $instance) {
            Logging::info("Removing gaps from show instance #".$instance->getDbId());
            //DateTime object
            $itemStart = $instance->getDbStarts(null);

            $ccScheduleItems = CcScheduleQuery::create()
                ->filterByDbInstanceId($instance->getDbId())
                ->filterByDbId($schedIds, Criteria::NOT_IN)
                ->orderByDbStarts()
                ->find();

            foreach ($ccScheduleItems as $ccSchedule) {
                //DateTime object
                $itemEnd = $this->findEndTime($itemStart, $ccSchedule->getDbClipLength());

                $ccSchedule->setDbStarts($itemStart)
                    ->setDbEnds($itemEnd);

                $itemStart = $itemEnd;
            }
            $ccScheduleItems->save();
        }
    }

    /**
     * 
     * Enter description here ...
     * @param DateTime $instanceStart
     * @param string $clipLength
     */
    private static function findEndTime($instanceStart, $clipLength)
    {
        $startEpoch = $instanceStart->format("U.u");
        $durationSeconds = Application_Common_DateHelper::playlistTimeToSeconds($clipLength);

        //add two float numbers to 6 subsecond precision
        //DateTime::createFromFormat("U.u") will have a problem if there is no decimal in the resulting number.
        $endEpoch = bcadd($startEpoch , (string) $durationSeconds, 6);

        $dt = DateTime::createFromFormat("U.u", $endEpoch, new DateTimeZone("UTC"));

        if ($dt === false) {
            //PHP 5.3.2 problem
            $dt = DateTime::createFromFormat("U", intval($endEpoch), new DateTimeZone("UTC"));
        }

        return $dt;
    }

    public static function fillNewLinkedInstances($ccShow)
    {
        /* First check if any linked instances have content
         * If all instances are empty then we don't need to fill
         * any other instances with content
         */
        $instanceIds = $ccShow->getInstanceIds();
        $schedule_sql = "SELECT * FROM cc_schedule ".
            "WHERE instance_id IN (".implode($instanceIds, ",").")";
        $ccSchedules = Application_Common_Database::prepareAndExecute(
            $schedule_sql);

        if (count($ccSchedules) > 0) {
            /* Find the show contents of just one of the instances. It doesn't
             * matter which instance we use since all the content is the same
             */
            $ccSchedule = $ccSchedules[0];
            $showStamp_sql = "SELECT * FROM cc_schedule ".
                "WHERE instance_id = {$ccSchedule["instance_id"]} ".
                "ORDER BY starts";
            $showStamp = Application_Common_Database::prepareAndExecute(
                $showStamp_sql);

            //get time_filled so we can update cc_show_instances
            $timeFilled_sql = "SELECT time_filled FROM cc_show_instances ".
                "WHERE id = {$ccSchedule["instance_id"]}";
            $timeFilled = Application_Common_Database::prepareAndExecute(
                $timeFilled_sql, array(), Application_Common_Database::COLUMN);

            //need to find out which linked instances are empty
            $values = array();
            foreach ($instanceIds as $id) {
                $instanceSched_sql = "SELECT * FROM cc_schedule ".
                    "WHERE instance_id = {$id} ".
                    "ORDER by starts";
                $ccSchedules = Application_Common_Database::prepareAndExecute(
                    $instanceSched_sql);

                /* If the show instance is empty OR it has different content than
                 * the first instance, we need to fill/replace with the show stamp
                 * (The show stamp is taken from the first show instance's content)
                 */
                if (count($ccSchedules) < 1 || 
                    self::replaceInstanceContentCheck($ccSchedules, $showStamp)) {

                    $instanceStart_sql = "SELECT starts FROM cc_show_instances ".
                        "WHERE id = {$id} ".
                        "ORDER BY starts";
                    $nextStartDT = new DateTime(
                        Application_Common_Database::prepareAndExecute(
                            $instanceStart_sql, array(), Application_Common_Database::COLUMN),
                        new DateTimeZone("UTC"));

                    foreach ($showStamp as $item) {
                        $endTimeDT = self::findEndTime($nextStartDT, $item["clip_length"]);

                        if (is_null($item["file_id"])) {
                            $item["file_id"] = "null";
                        } 
                        if (is_null($item["stream_id"])) {
                            $item["stream_id"] = "null";
                        }

                        $values[] = "(".
                            "'{$nextStartDT->format("Y-m-d H:i:s")}', ".
                            "'{$endTimeDT->format("Y-m-d H:i:s")}', ".
                            "'{$item["clip_length"]}', ".
                            "'{$item["fade_in"]}', ".
                            "'{$item["fade_out"]}', ".
                            "'{$item["cue_in"]}', ".
                            "'{$item["cue_out"]}', ".
                            "{$item["file_id"]}, ".
                            "{$item["stream_id"]}, ".
                            "{$id}, ".
                            "{$item["position"]})";

                        $nextStartDT = $endTimeDT;
                    } //foreach show item
                }
            } //foreach linked instance

            $insert_sql = "INSERT INTO cc_schedule (starts, ends, ".
                "clip_length, fade_in, fade_out, cue_in, cue_out, ".
                "file_id, stream_id, instance_id, position)  VALUES ".
                implode($values, ",");

            Application_Common_Database::prepareAndExecute(
                $insert_sql, array(), Application_Common_Database::EXECUTE);

            //update time_filled in cc_show_instances
            $now = gmdate("Y-m-d H:i:s");
            $update_sql = "UPDATE cc_show_instances SET ".
                "time_filled = '{$timeFilled}', ".
                "last_scheduled = '{$now}' ".
                "WHERE show_id = {$ccShow->getDbId()}";
            Application_Common_Database::prepareAndExecute(
                $update_sql, array(), Application_Common_Database::EXECUTE);

        } //if at least one linked instance has content
    }

    public static function fillPreservedLinkedShowContent($ccShow, $showStamp)
    {
        $item = $showStamp->getFirst();
        $timeFilled = $item->getCcShowInstances()->getDbTimeFilled();

        foreach ($ccShow->getCcShowInstancess() as $ccShowInstance) {
            $ccSchedules = CcScheduleQuery::create()
                ->filterByDbInstanceId($ccShowInstance->getDbId())
                ->find();

            if ($ccSchedules->isEmpty()) {

                $nextStartDT = $ccShowInstance->getDbStarts(null);

                foreach ($showStamp as $item) {
                    $endTimeDT = self::findEndTime($nextStartDT, $item->getDbClipLength());

                    $ccSchedule = new CcSchedule();
                    $ccSchedule
                        ->setDbStarts($nextStartDT)
                        ->setDbEnds($endTimeDT)
                        ->setDbFileId($item->getDbFileId())
                        ->setDbStreamId($item->getDbStreamId())
                        ->setDbClipLength($item->getDbClipLength())
                        ->setDbFadeIn($item->getDbFadeIn())
                        ->setDbFadeOut($item->getDbFadeOut())
                        ->setDbCuein($item->getDbCueIn())
                        ->setDbCueOut($item->getDbCueOut())
                        ->setDbInstanceId($ccShowInstance->getDbId())
                        ->setDbPosition($item->getDbPosition())
                        ->save();

                    $nextStartDT = $endTimeDT;
                } //foreach show item

                $ccShowInstance
                    ->setDbTimeFilled($timeFilled)
                    ->setDbLastScheduled(gmdate("Y-m-d H:i:s"))
                    ->save();
            }
        }
    }

    private static function replaceInstanceContentCheck($currentShowStamp, $showStamp)
    {
        /*$currentShowStamp = CcScheduleQuery::create()
            ->filterByDbInstanceId($ccShowInstance->getDbId())
            ->orderByDbStarts()
            ->find();*/

        $counter = 0;
        foreach ($showStamp as $item) {
            if ($item["file_id"] != $currentShowStamp[$counter]["file_id"] ||
                $item["stream_id"] != $currentShowStamp[$counter]["stream_id"]) {
                    /*CcScheduleQuery::create()
                        ->filterByDbInstanceId($ccShowInstance->getDbId())
                        ->delete();*/
                    $delete_sql = "DELETE FROM cc_schedule ".
                        "WHERE instance_id = {$currentShowStamp[$counter]["instance_id"]}";
                    Application_Common_Database::prepareAndExecute(
                        $delete_sql, array(), Application_Common_Database::EXECUTE);
                    return true;
                }
        }

        /* If we get here, the content in the show instance is the same
         * as what we want to replace it with, so we can leave as is
         */
        return false;
    }

    public function emptyShowContent($instanceId)
    {
        try {
            $ccShowInstance = CcShowInstancesQuery::create()->findPk($instanceId);

            $instances = array();
            $instanceIds = array();

            if ($ccShowInstance->getCcShow()->isLinked()) {
                foreach ($ccShowInstance->getCcShow()->getCcShowInstancess() as $instance) {
                    $instanceIds[] = $instance->getDbId();
                    $instances[] = $instance;
                }
            } else {
                $instanceIds[] = $ccShowInstance->getDbId();
                $instances[] = $ccShowInstance;
            }

            /* Get the file ids of the tracks we are about to delete
             * from cc_schedule. We need these so we can update the
             * is_scheduled flag in cc_files
             */
            $ccSchedules = CcScheduleQuery::create()
                ->filterByDbInstanceId($instanceIds, Criteria::IN)
                ->setDistinct(CcSchedulePeer::FILE_ID)
                ->find();
            $fileIds = array();
            foreach ($ccSchedules as $ccSchedule) {
                $fileIds[] = $ccSchedule->getDbFileId();
            }

            /* Clear out the schedule */
            CcScheduleQuery::create()
                ->filterByDbInstanceId($instanceIds, Criteria::IN)
                ->delete();

            /* Now that the schedule has been cleared we need to make
             * sure we do not update the is_scheduled flag for tracks
             * that are scheduled in other shows
             */
            $futureScheduledFiles = Application_Model_Schedule::getAllFutureScheduledFiles();
            foreach ($fileIds as $k => $v) {
                if (in_array($v, $futureScheduledFiles)) {
                    unset($fileIds[$k]);
                }
            }

            $selectCriteria = new Criteria();
            $selectCriteria->add(CcFilesPeer::ID, $fileIds, Criteria::IN);
            $updateCriteria = new Criteria();
            $updateCriteria->add(CcFilesPeer::IS_SCHEDULED, false);
            BasePeer::doUpdate($selectCriteria, $updateCriteria, Propel::getConnection());

            Application_Model_RabbitMq::PushSchedule();
            $con = Propel::getConnection(CcShowInstancesPeer::DATABASE_NAME);
            foreach ($instances as $instance) {
                $instance->updateDbTimeFilled($con);
            }

            return true;
        } catch (Exception $e) {
            Logging::info($e->getMessage());
            return false;
        }
    }

    public function insertAfter(
        $scheduleItems,
        $mediaItems,
        $filesToInsert = null,
        $adjustSched = true,
        $moveAction = false)
    {
        $this->con->beginTransaction();

        try {
            $this->validateRequest($scheduleItems, true);

            $affectedShowInstances = array();
            $excludePositions = array();
            $instance = null;

            /* Items in shows are ordered by position number. We need to know
             * the position when adding/moving items in linked shows so they are
             * added or moved in the correct position
             */
            $pos = 0;

            foreach ($scheduleItems as $schedule) {
                $scheduleId = intval($schedule["id"]);
                $instanceId = $schedule["instance"];

                if ($this->isDuplicateScheduleLocation($scheduleId, $instanceId)) {
                    continue;
                }

                foreach ($this->getInstances($instanceId) as $instance) {
                    $instanceId = $instance["id"];

                    list($nextStartDT, $applyCrossfades) = $this->getRelativeStartTime(
                        $scheduleId, $instance);

                    if (!in_array($instanceId, $affectedShowInstances)) {
                        $affectedShowInstances[] = $instanceId;
                    }

                    /*
                     * $adjustSched is true if there are schedule items
                     * following the item just inserted, per show instance
                     */
                    if ($adjustSched === true) {
                        if ($applyCrossfades) {
                            $initialStartDT = clone $this->applyCrossfades(
                                $nextStartDT, $this->crossfadeDuration);
                        } else {
                            $initialStartDT = clone $nextStartDT;
                        }
                    }

                    if (is_null($filesToInsert)) {
                        $filesToInsert = array();
                        foreach ($mediaItems as $media) {
                            $filesToInsert = array_merge($filesToInsert,
                                $this->retrieveMediaFiles($media["id"], $media["type"]));
                        }
                    }

                    $doInsert = false;
                    unset($this->insertValues);

                    foreach ($filesToInsert as &$file) {
                        if (isset($file['sched_id'])) {
                            $file = $this->prepareMovedItem($file);
                        } else {
                            $doInsert = true;
                        }

                        $file = $this->prepareScheduleData($file, $applyCrossfades,
                            $doInsert);
                    }

                    if ($doInsert) {
                        $this->excludeIds = $this->doScheduleInsert();
                    }

                    $this->updateFileScheduledFlag($filesToInsert);

                    /* Reset files to insert so we can get a new set of files. We have
                     * to do this in case we are inserting a dynamic block
                     */
                    if (!$moveAction) {
                        $filesToInsert = null;
                    }

                    if ($adjustSched === true) {
                        $this->adjustSchedule($initialStartDT, $instanceId,
                            $nextStartDT);
                    }

                }//foreach instance

            }//foreach scheduled item

            $this->con->commit();

            Application_Model_RabbitMq::PushSchedule();
        } catch (Exception $e) {
            $this->con->rollback();
            throw $e;
        }
    }

    private function adjustSchedule($initialStartDT, $instanceId, $nextStartDT)
    {
        $followingItems_sql = "SELECT * FROM cc_schedule ".
            "WHERE starts >= '{$initialStartDT->format("Y-m-d H:i:s.u")}' ".
            "AND instance_id = {$instanceId} ";
        if (count($excludeIds) > 0) {
            $followingItems_sql .= "AND id NOT IN (". implode($this->excludeIds, ",").") ";
        }
        $followingItems_sql .= "ORDER BY starts";
        $followingSchedItems = Application_Common_Database::prepareAndExecute(
            $followingItems_sql);

        $pstart = microtime(true);

        //recalculate the start/end times after the inserted items.
        foreach ($followingSchedItems as $item) {
            $endTimeDT = $this->findEndTime($nextStartDT, $item["clip_length"]);
            $endTimeDT = $this->applyCrossfades($endTimeDT, $this->crossfadeDuration);
            $update_sql = "UPDATE cc_schedule SET ".
                "starts = '{$nextStartDT->format("Y-m-d H:i:s")}', ".
                "ends = '{$endTimeDT->format("Y-m-d H:i:s")}', ".
                "position = {$pos} ".
                "WHERE id = {$item["id"]}";
            Application_Common_Database::prepareAndExecute(
                $update_sql, array(), Application_Common_Database::EXECUTE);

            $nextStartDT = $this->applyCrossfades($endTimeDT, $this->crossfadeDuration);
            $pos++;
        }
    }

    private function updateFileScheduledFlag($filesToInsert)
    {
        $fileIds = array();
        foreach ($filesToInsert as &$file) {
            $fileIds[] = $file["id"];
        }
        $selectCriteria = new Criteria();
        $selectCriteria->add(CcFilesPeer::ID, $fileIds, Criteria::IN);
        $selectCriteria->addAnd(CcFilesPeer::IS_SCHEDULED, false);
        $updateCriteria = new Criteria();
        $updateCriteria->add(CcFilesPeer::IS_SCHEDULED, true);
        BasePeer::doUpdate($selectCriteria, $updateCriteria, $this->con);
    }

    private function applyCrossfades($startDT, $seconds)
    {
        $startEpoch = $startDT->format("U.u");

        //add two float numbers to 6 subsecond precision
        //DateTime::createFromFormat("U.u") will have a problem if there is no decimal in the resulting number.
        $newEpoch = bcsub($startEpoch , (string) $seconds, 6);

        $dt = DateTime::createFromFormat("U.u", $newEpoch, new DateTimeZone("UTC"));

        if ($dt === false) {
            //PHP 5.3.2 problem
            $dt = DateTime::createFromFormat("U", intval($newEpoch), new DateTimeZone("UTC"));
        }

        return $dt;
    }

    private function doScheduleInsert()
    {
        $insertedIds = array();

        $insert_sql = "INSERT INTO cc_schedule ".
            "(starts, ends, cue_in, cue_out, fade_in, fade_out, ".
            "clip_length, position, instance_id, file_id, stream_id) VALUES ".
            implode($this->insertValues, ",")." RETURNING id";

        $stmt = $this->con->prepare($insert_sql);
        if ($stmt->execute()) {
            foreach ($stmt->fetchAll() as $row) {
                $this->excludeIds[] = $row["id"];
            }
        };
    }

    private function prepareScheduleData($file, $applyCrossfades, $doInsert)
    {
        $file['fadein'] = Application_Common_DateHelper::secondsToPlaylistTime(
            $file['fadein']);
        $file['fadeout'] = Application_Common_DateHelper::secondsToPlaylistTime(
            $file['fadeout']);

        switch ($file["type"]) {
            case 0:
                $fileId = $file["id"];
                $streamId = "null";
                break;
            case 1:
                $streamId = $file["id"];
                $fileId = "null";
                break;
            default: break;
        }

        if ($applyCrossfades) {
            $nextStartDT = $this->applyCrossfades($nextStartDT,
                $this->crossfadeDuration);
            $endTimeDT = $this->findEndTime($nextStartDT, $file['cliplength']);
            $endTimeDT = $this->applyCrossfades($endTimeDT, $this->crossfadeDuration);
            /* Set it to false because the rest of the crossfades
             * will be applied after we insert each item
             */
            $applyCrossfades = false;
        }

        $endTimeDT = $this->findEndTime($nextStartDT, $file['cliplength']);

        if ($doInsert) {
            $this->insertValues[] = "(".
                "'{$nextStartDT->format("Y-m-d H:i:s")}', ".
                "'{$endTimeDT->format("Y-m-d H:i:s")}', ".
                "'{$file["cuein"]}', ".
                "'{$file["cueout"]}', ".
                "'{$file["fadein"]}', ".
                "'{$file["fadeout"]}', ".
                "'{$file["cliplength"]}', ".
                "{$pos}, ".
                "{$instanceId}, ".
                "{$fileId}, ".
                "{$streamId})";

        } else {
            $update_sql = "UPDATE cc_schedule SET ".
                "starts = '{$nextStartDT->format("Y-m-d H:i:s")}', ".
                "ends = '{$endTimeDT->format("Y-m-d H:i:s")}', ".
                "cue_in = '{$file["cuein"]}', ".
                "cue_out = '{$file["cueout"]}', ".
                "fade_in = '{$file["fadein"]}', ".
                "fade_out = '{$file["fadeout"]}', ".
                "clip_length = '{$file["cliplength"]}', ".
                "position = {$pos} ".
                "WHERE id = {$sched["id"]}";

            Application_Common_Database::prepareAndExecute(
                $update_sql, array(), Application_Common_Database::EXECUTE);
        }

        $nextStartDT = $this->applyCrossfades($endTimeDT, $this->crossfadeDuration);
        $pos++;
    }

    private function prepareMovedItem($file)
    {
        $movedItem_sql = "SELECT * FROM cc_schedule ".
            "WHERE id = ".$file["sched_id"];
        $sched = Application_Common_Database::prepareAndExecute(
            $movedItem_sql, array(), Application_Common_Database::SINGLE);

        /* We need to keep a record of the original positon a track
         * is being moved from so we can use it to retrieve the correct
         * items in linked instances
         */
        if (!isset($originalPosition)) {
            $originalPosition = $sched["position"];
        }

        /* If we are moving an item in a linked show we need to get
         * the relative item to move in each instance. We know what the
         * relative item is by its position
         */
        if ($linked) {
            $movedItem_sql = "SELECT * FROM cc_schedule ".
                "WHERE position = {$originalPosition} ".
                "AND instance_id = {$instanceId}";

            $sched = Application_Common_Database::prepareAndExecute(
                $movedItem_sql, array(), Application_Common_Database::SINGLE);
        }
        $this->excludeIds[] = intval($sched["id"]);

        $file["cliplength"] = $sched["clip_length"];
        $file["cuein"] = $sched["cue_in"];
        $file["cueout"] = $sched["cue_out"];
        $file["fadein"] = $sched["fade_in"];
        $file["fadeout"] = $sched["fade_out"];

        return $file;
    }

    /**
     * 
     * This function ensures we are not making duplicate schedule entries.
     * This can happen when the user is trying to schedule items in two or
     * more show instances that are linked.
     * We keep track of the schedule locations with a unique id, which is made
     * up by the show id and the schedule position. If the show instances are
     * empty we use a character instead of the schedule position.
     * 
     * @param $id cc_schedule id
     * @param $instanceId
     */
    private function isDuplicateScheduleLocation($id, $instanceId)
    {
        if ($id != 0) {
            $schedule_sql = "SELECT * FROM cc_schedule WHERE id = ".$id;
            $ccSchedule = Application_Common_Database::prepareAndExecute(
                $schedule_sql, array(), Application_Common_Database::SINGLE);

            $this->position = $ccSchedule["position"];

            $show_sql = "SELECT * FROM cc_show WHERE id IN (".
                "SELECT show_id FROM cc_show_instances WHERE id = ".
                $ccSchedule["instance_id"].")";

            $ccShow = Application_Common_Database::prepareAndExecute(
                $show_sql, array(), Application_Common_Database::SINGLE);

            $this->ccShow = $ccShow;
            if ($this->ccShow["linked"]) {
                $unique = $ccShow["id"] . $ccSchedule["position"];
                if (!in_array($unique, $this->scheduleLocations)) {
                    $this->scheduleLocations[] = $unique;
                    return false;
                } else {
                    return true;
                }
            }
        } else {
            // first item in show so start position counter at 0
            $this->position = 0;

            $show_sql = "SELECT * FROM cc_show WHERE id IN (".
                "SELECT show_id FROM cc_show_instances WHERE id = ".
                $instanceId.")";

            $ccShow = Application_Common_Database::prepareAndExecute(
                $show_sql, array(), Application_Common_Database::SINGLE);

            $this->ccShow = $ccShow;
            if ($this->ccShow["linked"]) {
                $unique = $ccShow["id"] . "a";
                if (!in_array($unique, $this->scheduleLocations)) {
                    $this->scheduleLocations[] = $unique;
                    return false;
                } else {
                    return true;
                }
            }
        }
    }

    /**
     * 
     * If the show where the cursor position is located is linked this function
     * returns all the instances belonging to that show. Otherwise it returns
     * the single instance.
     */
    private function getInstances($instanceId)
    {
        if ($this->ccShow["linked"]) {
            $instance_sql = "SELECT * FROM cc_show_instances ".
                "WHERE show_id = ".$this->ccShow["id"];
            $instances = Application_Common_Database::prepareAndExecute(
                $instance_sql);
        } else {
            $instance_sql = "SELECT * FROM cc_show_instances ".
                "WHERE id = ".$instanceId;
            $instances = Application_Common_Database::prepareAndExecute(
                $instance_sql);
        }

        return $instances;
    }

    /**
     * 
     * This function finds the inserted item's start time. It is relative
     * if the user is inserting into a linked show because we have to use
     * the "after item's" position to find the linked instances insert location
     * 
     * @param $scheduleId cc_schedule id
     * @param $instance cc_show_instance
     */
    private function getRelativeStartTime($scheduleId, $instance)
    {
        if ($scheduleId !== 0) {

            $linkedItem_sql = "SELECT ends FROM cc_schedule ".
                "WHERE instance_id = {$instance["id"]} ".
                "AND position = {$this->position} ".
                "AND playout_status != -1";
            $linkedItemEnds = Application_Common_Database::prepareAndExecute(
                $linkedItem_sql, array(), Application_Common_Database::COLUMN);

            $this->position++;

            $nextStartDT = $this->findNextStartTime(
                new DateTime($linkedItemEnds, new DateTimeZone("UTC")),
                $instanceId);

            /* Return true here because show is not empty so we need to apply
             * crossfades for the first inserted item
             */
            return array($nextStartDT, true);
        }
        //selected empty row to add after
        else {
            $nextStartDT = $this->findNextStartTime(
                new DateTime($instance["starts"], new DateTimeZone("UTC")),
                $instanceId);

            /* Return false here because show is empty so we don't need to
             * calculate crossfades for the first inserted item
             */
            return array($nextStartDT, false);
        }
    }

    private function findNextStartTime($DT, $instanceId)
    {
        $sEpoch = $DT->format("U.u");
        $nEpoch = $this->epochNow;

        //check for if the show has started.
        if (bccomp( $nEpoch , $sEpoch , 6) === 1) {
            //need some kind of placeholder for cc_schedule.
            //playout_status will be -1.
            $nextDT = $this->nowDT;

            $length = bcsub($nEpoch , $sEpoch , 6);
            $cliplength = Application_Common_DateHelper::secondsToPlaylistTime($length);

            //fillers are for only storing a chunk of time space that has already passed.
            $filler = new CcSchedule();
            $filler->setDbStarts($DT)
                ->setDbEnds($this->nowDT)
                ->setDbClipLength($cliplength)
                ->setDbCueIn('00:00:00')
                ->setDbCueOut('00:00:00')
                ->setDbPlayoutStatus(-1)
                ->setDbInstanceId($instanceId)
                ->save($this->con);
        } else {
            $nextDT = $DT;
        }

        return $nextDT;
    }

    private function retrieveMediaFiles($id, $type)
    {
        $files = array();

        if ($type === "audioclip") {
            $file = CcFilesQuery::create()->findPK($id, $this->con);
            $storedFile = new Application_Model_StoredFile($file, $this->con);

            if (is_null($file) || !$file->visible()) {
                throw new Exception(_("A selected File does not exist!"));
            } else {
                $data = $this->fileInfo;
                $data["id"] = $id;
                $data["cliplength"] = $storedFile->getRealClipLength(
                    $file->getDbCuein(),
                    $file->getDbCueout());

                $data["cuein"] = $file->getDbCuein();
                $data["cueout"] = $file->getDbCueout();

                //fade is in format SS.uuuuuu
                $data["fadein"] = Application_Model_Preference::GetDefaultFadeIn();
                $data["fadeout"] = Application_Model_Preference::GetDefaultFadeOut();

                $files[] = $data;
            }
        } elseif ($type === "playlist") {
            $pl = new Application_Model_Playlist($id);
            $contents = $pl->getContents();

            foreach ($contents as $plItem) {
                if ($plItem['type'] == 0) {
                    $data["id"] = $plItem['item_id'];
                    $data["cliplength"] = $plItem['length'];
                    $data["cuein"] = $plItem['cuein'];
                    $data["cueout"] = $plItem['cueout'];
                    $data["fadein"] = $plItem['fadein'];
                    $data["fadeout"] = $plItem['fadeout'];
                    $data["type"] = 0;
                    $files[] = $data;
                } elseif ($plItem['type'] == 1) {
                    $data["id"] = $plItem['item_id'];
                    $data["cliplength"] = $plItem['length'];
                    $data["cuein"] = $plItem['cuein'];
                    $data["cueout"] = $plItem['cueout'];
                    $data["fadein"] = "00.500000";//$plItem['fadein'];
                    $data["fadeout"] = "00.500000";//$plItem['fadeout'];
                    $data["type"] = 1;
                    $files[] = $data;
                } elseif ($plItem['type'] == 2) {
                    // if it's a block
                    $bl = new Application_Model_Block($plItem['item_id']);
                    if ($bl->isStatic()) {
                        foreach ($bl->getContents() as $track) {
                            $data["id"] = $track['item_id'];
                            $data["cliplength"] = $track['length'];
                            $data["cuein"] = $track['cuein'];
                            $data["cueout"] = $track['cueout'];
                            $data["fadein"] = $track['fadein'];
                            $data["fadeout"] = $track['fadeout'];
                            $data["type"] = 0;
                            $files[] = $data;
                        }
                    } else {
                        $defaultFadeIn = Application_Model_Preference::GetDefaultFadeIn();
                        $defaultFadeOut = Application_Model_Preference::GetDefaultFadeOut();
                        $dynamicFiles = $bl->getListOfFilesUnderLimit();
                        foreach ($dynamicFiles as $f) {
                            $fileId = $f['id'];
                            $file = CcFilesQuery::create()->findPk($fileId);
                            if (isset($file) && $file->visible()) {
                                $data["id"] = $file->getDbId();
                                $data["cuein"] = $file->getDbCuein();
                                $data["cueout"] = $file->getDbCueout();

                                $cuein = Application_Common_DateHelper::calculateLengthInSeconds($data["cuein"]);
                                $cueout = Application_Common_DateHelper::calculateLengthInSeconds($data["cueout"]);
                                $data["cliplength"] = Application_Common_DateHelper::secondsToPlaylistTime($cueout - $cuein);
                                
                                //fade is in format SS.uuuuuu
                                $data["fadein"] = $defaultFadeIn;
                                $data["fadeout"] = $defaultFadeOut;
                                
                                $data["type"] = 0;
                                $files[] = $data;
                            }
                        }
                    }
                }
            }
        } elseif ($type == "stream") {
            //need to return
             $stream = CcWebstreamQuery::create()->findPK($id, $this->con);

            if (is_null($stream)) {
                throw new Exception(_("A selected File does not exist!"));
            } else {
                $data = $this->fileInfo;
                $data["id"] = $id;
                $data["cliplength"] = $stream->getDbLength();
                $data["cueout"] = $stream->getDbLength();
                $data["type"] = 1;

                //fade is in format SS.uuuuuu
                $data["fadein"] = Application_Model_Preference::GetDefaultFadeIn();
                $data["fadeout"] = Application_Model_Preference::GetDefaultFadeOut();

                $files[] = $data;
            }
        } elseif ($type == "block") {
            $bl = new Application_Model_Block($id);
            if ($bl->isStatic()) {
                foreach ($bl->getContents() as $track) {
                    $data["id"] = $track['item_id'];
                    $data["cliplength"] = $track['length'];
                    $data["cuein"] = $track['cuein'];
                    $data["cueout"] = $track['cueout'];
                    $data["fadein"] = $track['fadein'];
                    $data["fadeout"] = $track['fadeout'];
                    $data["type"] = 0;
                    $files[] = $data;
                }
            } else {
                $defaultFadeIn = Application_Model_Preference::GetDefaultFadeIn();
                $defaultFadeOut = Application_Model_Preference::GetDefaultFadeOut();
                $dynamicFiles = $bl->getListOfFilesUnderLimit();
                foreach ($dynamicFiles as $f) {
                    $fileId = $f['id'];
                    $file = CcFilesQuery::create()->findPk($fileId);
                    if (isset($file) && $file->visible()) {
                        $data["id"] = $file->getDbId();
                        $data["cuein"] = $file->getDbCuein();
                        $data["cueout"] = $file->getDbCueout();

                        $cuein = Application_Common_DateHelper::calculateLengthInSeconds($data["cuein"]);
                        $cueout = Application_Common_DateHelper::calculateLengthInSeconds($data["cueout"]);
                        $data["cliplength"] = Application_Common_DateHelper::secondsToPlaylistTime($cueout - $cuein);
                        
                        //fade is in format SS.uuuuuu
                		$data["fadein"] = $defaultFadeIn;
                		$data["fadeout"] = $defaultFadeOut;
                		
                        $data["type"] = 0;
                        $files[] = $data;
                    }
                }
            }
        }

        return $files;
    }

    private function organizeRequestData($items)
    {
        $schedInfo = array();
        $instanceInfo = array();

        foreach ($items as $item) {
            $id = $item["id"];

            //could be added to the beginning of a show, which sends id = 0;
            if ($id > 0) {
                $schedInfo[$id] = $item["instance"];
            }

            //format is instance_id => timestamp
            $instanceInfo[$item["instance"]] = $item["timestamp"];
        }

        return array($schedInfo, $instanceInfo);
    }

    /**
     * 
     * Enter description here ...
     * @param $items array containing pks of cc_schedule items
     * @param $addAction
     */
    private function validateRequest($items, $addAction=false)
    {
        list($schedInfo, $instanceInfo) = $this->organizeRequestData($items);

        if (count($instanceInfo) === 0) {
            throw new Exception("Invalid Request.");
        }

        $schedIds = array();
        if (count($schedInfo) > 0) {
            $schedIds = array_keys($schedInfo);
        }
        $schedItems = CcScheduleQuery::create()->findPKs($schedIds, $this->con);
        $instanceIds = array_keys($instanceInfo);
        $showInstances = CcShowInstancesQuery::create()->findPKs($instanceIds, $this->con);

        //an item has been deleted
        if (count($schedIds) !== count($schedItems)) {
            throw new OutDatedScheduleException(_("The schedule you're viewing is out of date! (sched mismatch)"));
        }

        //a show has been deleted
        if (count($instanceIds) !== count($showInstances)) {
            throw new OutDatedScheduleException(_("The schedule you're viewing is out of date! (instance mismatch)"));
        }

        foreach ($schedItems as $schedItem) {
            $id = $schedItem->getDbId();
            $instance = $schedItem->getCcShowInstances($this->con);

            if (intval($schedInfo[$id]) !== $instance->getDbId()) {
                throw new OutDatedScheduleException(_("The schedule you're viewing is out of date!"));
            }
        }

        $service_user = new Application_Service_UserService();
        $currentUser = $service_user->getCurrentUser();

        foreach ($showInstances as $instance) {

            $id = $instance->getDbId();
            $show = $instance->getCcShow($this->con);

            if (!$currentUser->isAdminOrPM() && !$currentUser->isHostOfShow($show->getDbId())) {
                throw new Exception(sprintf(_("You are not allowed to schedule show %s."), $show->getDbName()));
            }

            if ($instance->getDbRecord()) {
                throw new Exception(_("You cannot add files to recording shows."));
            }

            $showEndEpoch = floatval($instance->getDbEnds("U.u"));

            if ($showEndEpoch < $this->epochNow) {
                throw new OutDatedScheduleException(sprintf(_("The show %s is over and cannot be scheduled."), $show->getDbName()));
            }

            $ts = intval($instanceInfo[$id]);
            $lastSchedTs = intval($instance->getDbLastScheduled("U")) ? : 0;
            if ($ts < $lastSchedTs) {
                Logging::info("ts {$ts} last sched {$lastSchedTs}");
                throw new OutDatedScheduleException(sprintf(_("The show %s has been previously updated!"), $show->getDbName()));
            }

            /*
             * Does the afterItem belong to a show that is linked AND
             * currently playing?
             * If yes, throw an exception
             */
            if ($addAction) {
                $ccShow = $instance->getCcShow();
                if ($ccShow->isLinked()) {
                    //get all the linked shows instances and check if
                    //any of them are currently playing
                    $ccShowInstances = $ccShow->getCcShowInstancess();
                    $timeNowUTC = gmdate("Y-m-d H:i:s");
                    foreach ($ccShowInstances as $ccShowInstance) {

                        if ($ccShowInstance->getDbStarts() <= $timeNowUTC &&
                            $ccShowInstance->getDbEnds() > $timeNowUTC) {
                            throw new Exception(_("Content in linked shows must be scheduled before or after any one is broadcasted"));
                        }
                    }
                }
            }
        }
    }
}