{% assign dimensions = vars.facebook_ads.models.facebook__ad_report.dimensions %}
{% assign active_dimensions = dimensions | where: 'active', true %}
{% assign dimensions = dimensions | where: 'default', true | concat: active_dimensions  %}
{% assign metrics = vars.facebook_ads.models.facebook__ad_report.metrics %}
{% assign active_metrics = metrics | where: 'active', true %}
{% assign metrics = metrics | where: 'default', true | concat: active_metrics  %}
{% assign account_id = vars.facebook_ads.account_ids %}
{% assign active = vars.facebook_ads.active %}
{% assign table_active = vars.facebook_ads.models.facebook__ad_report.active %}
{% assign dataset_id = vars.output_dataset_id %}
{% assign table_id = vars.facebook_ads.models.facebook__ad_report.table_id %}
{% assign source_dataset_id = vars.facebook_ads.source_dataset_id %}
{% assign conversions = vars.facebook_ads.conversions %}
{% assign number_of_accounts = vars.facebook_ads.account_ids | size %}
{% assign regexp_filters = vars.facebook_ads.regexp_filters %}
{% assign has_actions = vars.facebook_ads.actions %}
{% assign has_action_values = vars.facebook_ads.action_values %}
{% assign source_table_id = 'ad_report' %}
{% assign ad_set_delimiter = vars.facebook_ads.delimiters.ad_set %}
{% assign campaign_delimiter = vars.facebook_ads.delimiters.campaign %}

CREATE OR REPLACE TABLE 
    `{{dataset_id}}`.`{{table_id}}` (
    {% for dimension in dimensions %}
        {% unless forloop.first %}
            , 
        {% endunless %}
        `{{dimension.name}}` {{dimension.type}} OPTIONS (description = '[db_field_name = {{dimension.name}}]') 
    {% endfor %}
    {% for metric in metrics %}
        , `{{metric.name}}` {{metric.type}}  OPTIONS (description = '[db_field_name = {{metric.name}}]') 
    {% endfor %}
    )
{% if active and table_active %}
    AS(    
        with 
        
        sync_info as (
            select
              max(datetime(_fivetran_synced, "{{ vars.timezone }}")) as max_synced_at
              , max(date) as max_data_date
            from {{source_dataset_id}}.{{source_table_id}}
        )
        
        , campaigns as(
            select
                *
            from
               {{source_dataset_id}}.campaign_history
            qualify rank() over(partition by id order by updated_time desc) = 1
            and row_number() over(partition by id, updated_time) = 1
        )

        , accounts as(
            select
                *
            from
                {{source_dataset_id}}.account_history
            qualify rank() over(partition by id order by _fivetran_synced desc) = 1
            and row_number() over(partition by id, _fivetran_synced) = 1
        )
        
        , ad_sets as(
            select
                *
            from
               {{source_dataset_id}}.ad_set_history
            qualify rank() over(partition by id order by updated_time desc) = 1
            and row_number() over(partition by id, updated_time) = 1
        )
        
        , ads as(
            select
                *
            from
               {{source_dataset_id}}.ad_history
            qualify rank() over(partition by id order by updated_time desc) = 1
            and row_number() over(partition by id, updated_time) = 1
        )

        , creatives as(
            select
                *
                , `gn-shared-services-production`.utilities.parseQueryParams(json_value(asset_feed_spec_link_urls, "$[0].website_url")) as parsed_utms
            from
                {{source_dataset_id}}.creative_history
            qualify rank() over(partition by id order by _fivetran_synced desc) = 1
            and row_number() over(partition by id, _fivetran_synced) = 1
        )

        , most_recent_ad_images as(
            select
                split(id, ':')[safe_ordinal(2)] as image_hash
                , permalink_url
                , url_128
                , updated_time
            from
                {{source_dataset_id}}.ad_image_history
            qualify rank() over(partition by image_hash order by updated_time desc) = 1
            and row_number() over(partition by image_hash, updated_time) = 1
        )

        , creative_with_ad_images as(
            select
                creatives.*
                , json_value(parsed_utms, "$.utm_campaign") as utm_campaign
                , most_recent_ad_images.permalink_url as image_permalink_url
                , most_recent_ad_images.url_128 as image_url_128
            from
                creatives
            left join
                most_recent_ad_images
            on
                coalesce(creatives.image_hash, '') = coalesce(most_recent_ad_images.image_hash, '999')
        )
        
        , stats as(
            select
                date
                , ad_id
                , sum(spend) as cost
                , sum(clicks) as clicks
                , sum(impressions) as impressions
                , sum(0) as frequency
                , sum(0) as reach
                , 0 as conversions
            from
               {{source_dataset_id}}.{{source_table_id}}
            group by
                1,2
        )
        
        {% if has_actions == true %}
            , actions as(
                select
                    *
                from
                    (select ad_id, date, action_type, value from {{source_dataset_id}}.{{source_table_id}}_actions)
                pivot(sum(value) as actions FOR action_type in (
                    {% for conversion in conversions %}
                        {% if forloop.first %}
                            '{{conversion.event_name}}' as {{conversion.output_name}}
                        {% else %}
                            , '{{conversion.event_name}}' as {{conversion.output_name}}
                        {% endif %}
                    {% endfor %}
                    )
                )
            )
        {% endif %}
        
        {% if has_action_values == true %}
            , action_values as(
                select
                    *
                from
                    (select ad_id, date, action_type, value from {{source_dataset_id}}.{{source_table_id}}_action_values)
                pivot(sum(value) as action_values FOR action_type in (
                    {% for conversion in conversions %}
                        {% if forloop.first %}
                            '{{conversion.event_name}}' as {{conversion.output_name}}
                        {% else %}
                            , '{{conversion.event_name}}' as {{conversion.output_name}}
                        {% endif %}
                    {% endfor %}
                    )
                )
            )
        {% endif %}
        
        , api as(
            select
                stats.* except(ad_id)
                , {{has_actions}} as has_actions
                , {{has_action_values}} as has_action_values
                , stats.ad_id
                , ads.name as ad_name
                , ad_sets.id as ad_set_id
                , ad_sets.name as ad_set_name
                , campaigns.id as campaign_id
                , campaigns.name as campaign_name
                /*
                    Campaign Delimiters. People use this if they have defined a taxonomy in their campaign
                    names where each position is like a category
                */
                , trim(split(campaigns.name, '{{campaign_delimiter}}')[safe_ordinal(1)]) as campaign_pos_1
                , trim(split(campaigns.name, '{{campaign_delimiter}}')[safe_ordinal(2)]) as campaign_pos_2
                , trim(split(campaigns.name, '{{campaign_delimiter}}')[safe_ordinal(3)]) as campaign_pos_3
                , trim(split(campaigns.name, '{{campaign_delimiter}}')[safe_ordinal(4)]) as campaign_pos_4
                , trim(split(campaigns.name, '{{campaign_delimiter}}')[safe_ordinal(5)]) as campaign_pos_5
                
                /*
                    Ad Set Delimiters. People use this if they have defined a taxonomy in their campaign
                    names where each position is like a category
                */
                , trim(split(ad_sets.name, '{{ad_set_delimiter}}')[safe_ordinal(1)]) as ad_set_pos_1
                , trim(split(ad_sets.name, '{{ad_set_delimiter}}')[safe_ordinal(2)]) as ad_set_pos_2
                , trim(split(ad_sets.name, '{{ad_set_delimiter}}')[safe_ordinal(3)]) as ad_set_pos_3
                , trim(split(ad_sets.name, '{{ad_set_delimiter}}')[safe_ordinal(4)]) as ad_set_pos_4
                , trim(split(ad_sets.name, '{{ad_set_delimiter}}')[safe_ordinal(5)]) as ad_set_pos_5
                
                , accounts.id as account_id
                , accounts.name as account_name
                , ads.creative_id
                , creative_with_ad_images.effective_object_story_id as post_id
                , creative_with_ad_images.body as post_body
                , CONCAT('https://www.facebook.com/', creative_with_ad_images.effective_object_story_id) as post_url
                , creative_with_ad_images.image_permalink_url
                , date(campaigns.start_time) as campaign_start_date
                , date(campaigns.stop_time) as campaign_end_date
                {% if has_actions == true %}
                    , actions.* except(ad_id, date)
                {% endif %}
                {% if has_action_values == true %}
                    , action_values.* except(ad_id, date)
                {% endif %}
                , sync_info.max_synced_at as last_synced_at
                , sync_info.max_data_date as last_data_date
                
            from
                stats
            left join
                ads
            on
                safe_cast(stats.ad_id as string) = safe_cast(ads.id as string)
            left join
                ad_sets
            on
                ads.ad_set_id = ad_sets.id
            left join
                campaigns
            on
                ad_sets.campaign_id = campaigns.id
            left join
                accounts
            on
                campaigns.account_id = accounts.id
            left join
                creative_with_ad_images
            on
                ads.creative_id = creative_with_ad_images.id
            {% if has_actions == true %}
                left join
                    actions
                on
                    stats.date = actions.date
                and
                    safe_cast(stats.ad_id as string) = safe_cast(actions.ad_id as string)
            {% endif %}
            {% if has_action_values == true %}
                left join
                    action_values
                on
                    stats.date = action_values.date
                and
                    safe_cast(stats.ad_id as string) = safe_cast(action_values.ad_id as string)
            {% endif %}
            left join
                sync_info
            on
                true
        )
        
        select
            {% for dimension in dimensions %}
                {% unless forloop.first %}
                    , 
                {% endunless %}
                CAST({{dimension.expression}} as {{dimension.type}}) as `{{dimension.name}}`
            {% endfor %}
            {% for metric in metrics %}
                , CAST({{metric.expression}} as {{metric.type}}) as `{{metric.name}}`
            {% endfor %}
        from
            api
            {% if number_of_accounts > 0 %}
                where account_id in(
                    {% for id in account_id %}
                        {% unless forloop.first %}
                            , 
                        {% endunless %}
                        {{id}}
                    {% endfor %}
                )
            {% endif %}
            {% if regexp_filters != blank and regexp_filters != false %}
                {% if number_of_accounts > 0 %}
                    AND
                {% else %}
                    WHERE
                {% endif %}
                {% for filter in regexp_filters %}
                    {% unless forloop.first %}
                        AND 
                    {% endunless %}
                    REGEXP_CONTAINS({{filter.dimension}}, r"{{filter.expression}}") = {{filter.result}}
                {% endfor %}
            {% endif %}
        group by
            {% for dimension in dimensions %}
                {% unless forloop.first %}
                    , 
                {% endunless %}
                {{forloop.index}}
            {% endfor %}
    )
{% endif %}
;
