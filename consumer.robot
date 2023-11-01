*** Settings ***
Documentation       Inhuman Insurance, Inc. Artificial Intelligence System robot.
...                 Consumes traffic data work items.

Resource            shared.robot
Library             RPA.HTTP


*** Tasks ***
Consume traffic data work items
    For Each Input Work Item    Process traffic data


*** Keywords ***
Process traffic data
    ${payload}=    Get Work Item Payload
    ${traffic_data}=    Set Variable    ${payload}[${WORK_ITEM_NAME}]
    ${valid}=    Validate traffic data    ${traffic_data}
    IF    ${valid}
        Post traffic data to sales system    ${traffic_data}
    ELSE
        Handle invalid traffic data    ${traffic_data}
    END

Execute Traffic Data Post
    [Arguments]    ${traffic_data}
    ${status}    ${return}=    Run Keyword And Ignore Error
    ...    POST
    ...    https://robocorp.com/inhuman-insurance-inc/sales-system-api
    ...    json=${traffic_data}
    RETURN    ${status}    ${return}

Validate traffic data
    [Arguments]    ${traffic_data}
    ${country}=    Get Value From Json    ${traffic_data}    $.country
    ${valid}=    Evaluate    len("${country}") == 3
    RETURN    ${valid}

Post traffic data to sales system
    [Arguments]    ${traffic_data}
    ${status}    ${return}=    Execute Traffic Data Post    ${traffic_data}
    Handle traffic API response    ${status}    ${return}    ${traffic_data}

Handle traffic API response
    [Arguments]    ${status}    ${return}    ${traffic_data}
    IF    "${status}" == "PASS"
        Handle traffic API OK response
    ELSE
        Handle traffic API error response    ${return}    ${traffic_data}
    END

Handle traffic API OK response
    Release Input Work Item    DONE

Handle traffic API error response
    [Arguments]    ${return}    ${traffic_data}
    ${status}    ${return}=    Retry On Application Error    ${traffic_data}
    IF    '${status}' == 'FAIL'
        Log
        ...    Traffic data posting failed: ${traffic_data} ${return}
        ...    ERROR
        Release Input Work Item
        ...    state=FAILED
        ...    exception_type=APPLICATION
        ...    code=TRAFFIC_DATA_POST_FAILED
        ...    message=${return}
    END

Handle invalid traffic data
    [Arguments]    ${traffic_data}
    ${corrected_data}=    Correct Traffic Data    ${traffic_data}
    IF    "${corrected_data}" != "None"
        Log    Corrected traffic data: ${corrected_data}
        ${status}    ${return}=    Execute Traffic Data Post    ${corrected_data}
        IF    "${status}" == "PASS"
            Release Input Work Item    DONE
            RETURN
        END
    END
    ${message}=    Set Variable    Invalid traffic data: ${traffic_data}
    Log    ${message}    WARN
    Release Input Work Item
    ...    state=FAILED
    ...    exception_type=BUSINESS
    ...    code=INVALID_TRAFFIC_DATA
    ...    message=${message}

Retry On Application Error
    [Arguments]    ${traffic_data}
    ${retry_count}=    Set Variable    7
    FOR    ${i}    IN RANGE    ${retry_count}
        ${status}    ${return}=    Execute Traffic Data Post    ${traffic_data}
        IF    '${status}' == 'PASS'
            Log    Reprocessed successfully after ${i+1} attempts: ${traffic_data}
            BREAK
        END
        # Sleep    1 seconds
    END
    RETURN    ${status}    ${return}

Correct Traffic Data
    [Arguments]    ${traffic_data}
    ${country}=    Get Value From Json    ${traffic_data}    $.country
    ${country_length}=    Get Length    ${country}
    IF    ${country_length} > 3
        Log    Invalid country detected: ${country}. Correcting to BRA.    WARN
        ${traffic_data}[country]=    Set Variable    BRA
        Sleep    3
        Save Work Item
        RETURN    ${traffic_data}
    END
