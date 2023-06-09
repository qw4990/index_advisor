create database gharchive_dev;
use gharchive_dev;

CREATE TABLE `github_events` (
                                 `id` bigint(20) DEFAULT NULL,
                                 `type` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                 `created_at` datetime DEFAULT NULL,
                                 `repo_id` bigint(20) DEFAULT NULL,
                                 `repo_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                 `actor_id` bigint(20) DEFAULT NULL,
                                 `actor_login` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                 `actor_location` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                 `language` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                 `additions` bigint(20) DEFAULT NULL,
                                 `deletions` bigint(20) DEFAULT NULL,
                                 `action` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                 `number` int(11) DEFAULT NULL,
                                 `commit_id` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                 `comment_id` bigint(20) DEFAULT NULL,
                                 `org_login` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                 `org_id` bigint(20) DEFAULT NULL,
                                 `state` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                 `closed_at` datetime DEFAULT NULL,
                                 `comments` int(11) DEFAULT NULL,
                                 `pr_merged_at` datetime DEFAULT NULL,
                                 `pr_merged` tinyint(1) DEFAULT NULL,
                                 `pr_changed_files` int(11) DEFAULT NULL,
                                 `pr_review_comments` int(11) DEFAULT NULL,
                                 `pr_or_issue_id` bigint(20) DEFAULT NULL,
                                 `event_day` date DEFAULT NULL,
                                 `event_month` date DEFAULT NULL,
                                 `author_association` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                 `event_year` int(11) DEFAULT NULL,
                                 `push_size` int(11) DEFAULT '2',
                                 `push_distinct_size` int(11) DEFAULT NULL,
                                 KEY `index_github_events_on_id` (`id`),
                                 KEY `index_github_events_on_action` (`action`),
                                 KEY `index_github_events_on_actor_id` (`actor_id`),
                                 KEY `index_github_events_on_actor_login` (`actor_login`),
                                 KEY `index_github_events_on_additions` (`additions`),
                                 KEY `index_github_events_on_closed_at` (`closed_at`),
                                 KEY `index_github_events_on_comment_id` (`comment_id`),
                                 KEY `index_github_events_on_comments` (`comments`),
                                 KEY `index_github_events_on_commit_id` (`commit_id`),
                                 KEY `index_github_events_on_created_at` (`created_at`),
                                 KEY `index_github_events_on_deletions` (`deletions`),
                                 KEY `index_github_events_on_event_day` (`event_day`),
                                 KEY `index_github_events_on_event_month` (`event_month`),
                                 KEY `index_github_events_on_event_year` (`event_year`),
                                 KEY `index_github_events_on_language` (`language`),
                                 KEY `index_github_events_on_org_id` (`org_id`),
                                 KEY `index_github_events_on_org_login` (`org_login`),
                                 KEY `index_github_events_on_pr_changed_files` (`pr_changed_files`),
                                 KEY `index_github_events_on_pr_merged_at` (`pr_merged_at`),
                                 KEY `index_github_events_on_pr_or_issue_id` (`pr_or_issue_id`),
                                 KEY `index_github_events_on_pr_review_comments` (`pr_review_comments`),
                                 KEY `index_github_events_on_repo_id` (`repo_id`),
                                 KEY `index_github_events_on_repo_name` (`repo_name`),
                                 KEY `index_github_events_on_type` (`type`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
PARTITION BY LIST COLUMNS(`type`)
(PARTITION `push_event` VALUES IN ("PushEvent"),
 PARTITION `create_event` VALUES IN ("CreateEvent"),
 PARTITION `pull_request_event` VALUES IN ("PullRequestEvent"),
 PARTITION `watch_event` VALUES IN ("WatchEvent"),
 PARTITION `issue_comment_event` VALUES IN ("IssueCommentEvent"),
 PARTITION `issues_event` VALUES IN ("IssuesEvent"),
 PARTITION `delete_event` VALUES IN ("DeleteEvent"),
 PARTITION `fork_event` VALUES IN ("ForkEvent"),
 PARTITION `pull_request_review_comment_event` VALUES IN ("PullRequestReviewCommentEvent"),
 PARTITION `pull_request_review_event` VALUES IN ("PullRequestReviewEvent"),
 PARTITION `gollum_event` VALUES IN ("GollumEvent"),
 PARTITION `release_event` VALUES IN ("ReleaseEvent"),
 PARTITION `member_event` VALUES IN ("MemberEvent"),
 PARTITION `commit_comment_event` VALUES IN ("CommitCommentEvent"),
 PARTITION `public_event` VALUES IN ("PublicEvent"),
 PARTITION `gist_event` VALUES IN ("GistEvent"),
 PARTITION `follow_event` VALUES IN ("FollowEvent"),
 PARTITION `event` VALUES IN ("Event"),
 PARTITION `download_event` VALUES IN ("DownloadEvent"),
 PARTITION `team_add_event` VALUES IN ("TeamAddEvent"),
 PARTITION `fork_apply_event` VALUES IN ("ForkApplyEvent"));


CREATE TABLE `github_repo_languages` (
                                         `repo_id` int(11) NOT NULL,
                                         `language` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL,
                                         `size` int(11) DEFAULT NULL,
                                         PRIMARY KEY (`repo_id`,`language`) /*T![clustered_index] CLUSTERED */
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `github_repos` (
                                `repo_id` int(11) NOT NULL,
                                `repo_name` varchar(150) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                `owner_id` int(11) DEFAULT NULL,
                                `owner_login` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                                `owner_is_org` tinyint(1) DEFAULT NULL,
                                `primary_language` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                `license` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                `size` bigint(20) DEFAULT NULL,
                                `stars` int(11) DEFAULT NULL,
                                `forks` int(11) DEFAULT NULL,
                                `parent_repo_id` int(11) DEFAULT NULL,
                                `is_fork` tinyint(1) NOT NULL DEFAULT '0',
                                `is_archived` tinyint(1) NOT NULL DEFAULT '0',
                                `is_deleted` tinyint(1) NOT NULL DEFAULT '0',
                                `latest_released_at` timestamp NULL DEFAULT NULL,
                                `pushed_at` timestamp NULL DEFAULT NULL,
                                `created_at` timestamp NULL DEFAULT NULL,
                                `updated_at` timestamp NULL DEFAULT NULL,
                                `refreshed_at` timestamp NULL DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
                                PRIMARY KEY (`repo_id`) /*T![clustered_index] CLUSTERED */,
                                KEY `index_owner_on_github_repos` (`owner_login`),
                                KEY `index_fullname_on_github_repos` (`repo_name`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `github_repo_topics` (
                                      `repo_id` int(11) NOT NULL,
                                      `topic` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL,
                                      PRIMARY KEY (`repo_id`,`topic`) /*T![clustered_index] CLUSTERED */
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE `github_users` (
                                `id` int(11) NOT NULL AUTO_INCREMENT,
                                `login` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL,
                                `company` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                `company_name` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
                                `type` varchar(255) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'USR',
                                `fake` tinyint(1) NOT NULL DEFAULT '0',
                                `deleted` tinyint(1) NOT NULL DEFAULT '0',
                                `long` decimal(11,8) DEFAULT NULL,
                                `lat` decimal(10,8) DEFAULT NULL,
                                `country_code` char(3) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                `state` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                `city` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                `location` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT NULL,
                                PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */,
                                KEY `index_login_on_users` (`login`),
                                KEY `idx_company_name` (`company_name`),
                                KEY `users_cmp_idx` (`company`)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci AUTO_INCREMENT=68024323;