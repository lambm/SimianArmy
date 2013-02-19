/*
 *
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.simianarmy.aws.janitor.rule.instance;

import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.simianarmy.MonkeyCalendar;
import com.netflix.simianarmy.Resource;
import com.netflix.simianarmy.aws.AWSResource;
import com.netflix.simianarmy.aws.janitor.crawler.InstanceJanitorCrawler;
import com.netflix.simianarmy.janitor.Rule;

/**
 * The rule for instances which don't have an owner.
 */
public class NoOwnerInstanceRule implements Rule {

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(NoOwnerInstanceRule.class);

    private static final String TERMINATION_REASON = "No ownerEmail tag associated with this instance";

    private final MonkeyCalendar calendar;

    private final int retentionDays;


    /**
     * Constructor for NoOwnerInstanceRule.
     *
     * @param calendar
     *            The calendar used to calculate the termination time
     * @param retentionDays
     *            The number of days that the instance is retained before being terminated
     */
    public NoOwnerInstanceRule(MonkeyCalendar calendar,
            int retentionDays) {
        Validate.notNull(calendar);
        Validate.isTrue(retentionDays >= 0);
        this.calendar = calendar;
        this.retentionDays = retentionDays;
    }

    @Override
    public boolean isValid(Resource resource) {
        Validate.notNull(resource);
        if (!resource.getResourceType().name().equals("INSTANCE")) {
            // The rule is supposed to only work on AWS instances. If a non-instance resource
            // is passed to the rule, the rule simply ignores it and considers it as a valid
            // resource not for cleanup.
            return true;
        }
        String awsStatus = ((AWSResource) resource).getAWSResourceState();
        if (!"running".equals(awsStatus) || "pending".equals(awsStatus)) {
            return true;
        }
        AWSResource instanceResource = (AWSResource) resource;
        String ownerTag = instanceResource.getTag(AWSResource.FIELD_OWNER_EMAIL);
        if (ownerTag == null || ownerTag.isEmpty()) {
            DateTime now = new DateTime(calendar.now().getTimeInMillis());
            Date terminationTime = calendar.getBusinessDay(new Date(now.getMillis()), retentionDays);
            resource.setExpectedTerminationTime(terminationTime);
            resource.setTerminationReason(TERMINATION_REASON);
            LOGGER.info(String.format("The instance %s has no ownerEmail tag",
                        resource.getId()));
            return false;
            
        }
        return true;
    }
}
