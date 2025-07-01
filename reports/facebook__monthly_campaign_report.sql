{% assign dimensions = vars.facebook_ads.models.facebook__monthly_campaign_report.dimensions %}
{% assign active_dimensions = dimensions | where: 'active', true %}
{% assign dimensions = dimensions | where: 'default', true | concat: active_dimensions  %}
{% assign metrics = vars.facebook_ads.models.facebook__monthly_campaign_report.metrics %}
{% assign active_metrics = metrics | where: 'active', true %}
{% assign metrics = metrics | where: 'default', true | concat: active_metrics  %}
{% assign account_id = vars.facebook_ads.account_ids %}
{% assign active = vars.facebook_ads.active %}
{% assign table_active = vars.facebook_ads.models.facebook__monthly_campaign_report.active %}
{% assign dataset_id = vars.output_dataset_id %}
{% assign table_id = 'facebook__monthly_campaign_report' %}
{% assign source_dataset_id = vars.facebook_ads.source_dataset_id %}
{% assign conversions = vars.facebook_ads.conversions %}
{% assign number_of_accounts = vars.facebook_ads.account_ids | size %}
{% assign regexp_filters = vars.facebook_ads.regexp_filters %}
{% assign has_actions = vars.facebook_ads.actions %}
{% assign has_action_values = vars.facebook_ads.action_values %}
{% assign source_table_id = 'monthly_campaign_report' %}
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
              max(current_datetime()) as max_synced_at
              , max(date) as max_data_date
            from {{source_dataset_id}}.{{source_table_id}}
        )
        
        , campaigns as(
            select
                *
            from
               {{source_dataset_id}}.campaign_history
            where is_current = true
        )

        , accounts as(
            select
                *
            from
                {{source_dataset_id}}.account_history
            where is_current = true
        )
        
        , stats as(
            select
                date
                , campaign_id
                , account_id
                , sum(spend) as cost
                , sum(clicks) as clicks
                , sum(impressions) as impressions
                , sum(frequency) as frequency
                , sum(reach) as reach
                , 0 as conversions
            from
               {{source_dataset_id}}.{{source_table_id}}
            group by
                1,2,3
        )
        
        {% if has_actions == true %}
            , actions as(
                select
                    *
                from
                    (select campaign_id, date, action_type, value from {{source_dataset_id}}.{{source_table_id}}_actions)
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
                    (select campaign_id, date, action_type, value from {{source_dataset_id}}.{{source_table_id}}_action_values)
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
                stats.* except(campaign_id, account_id)
                , {{has_actions}} as has_actions
                , {{has_action_values}} as has_action_values
                , stats.campaign_id
                , campaigns.name as campaign_name
                
                , stats.account_id as account_id
                , accounts.name as account_name
                {% if has_actions == true %}
                    , actions.* except(campaign_id, date)
                {% endif %}
                {% if has_action_values == true %}
                    , action_values.* except(campaign_id, date)
                {% endif %}
                , sync_info.max_synced_at as last_synced_at
                , sync_info.max_data_date as last_data_date
                
            from
                stats
            left join
                campaigns
            on
                safe_cast(stats.campaign_id as string) = safe_cast(campaigns.id as string)
            left join
                accounts
            on
                safe_cast(campaigns.account_id as string) = safe_cast(accounts.account_id as string)
            {% if has_actions == true %}
                left join
                    actions
                on
                    stats.date = actions.date
                and
                    safe_cast(stats.campaign_id as string) = safe_cast(actions.campaign_id as string)
            {% endif %}
            {% if has_action_values == true %}
                left join
                    action_values
                on
                    stats.date = action_values.date
                and
                    safe_cast(stats.campaign_id as string) = safe_cast(action_values.campaign_id as string)
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
                        '{{id}}'
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
