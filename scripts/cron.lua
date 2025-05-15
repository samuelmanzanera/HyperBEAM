function compute(process, message, opts)
    -- Early return when no body is provided
    -- This handles the initial invocation during process setup
    if not message or not message.body or not message.body.body then
        ao.event("debug_cron", { "error", "Initial message structure invalid", raw_message_body = message.body })
        return process
    end
    local cmd_body = message.body.body

    if not cmd_body.path or type(cmd_body.path) ~= "string" then
        ao.event("debug_cron", { "error", "Command body missing or invalid 'path'", received_body = cmd_body })
        return process 
    end
    local command = cmd_body.path
    ao.event("debug_cron", { "compute command", command })

    -- Initialize crons table if it doesn't exist
    process.crons = process.crons or {}

    if command == "put" then
        if not cmd_body.task_id or type(cmd_body.task_id) ~= "string" then
            ao.event("debug_cron", { "error", "Invalid 'put': missing/invalid task_id", command_body = cmd_body })
            return process
        end
        if not cmd_body.data or type(cmd_body.data) ~= "table" then
            ao.event("debug_cron", { "error", "Invalid 'put': missing/invalid data", command_body = cmd_body })
            return process
        end
        -- Standard addition to the cache with task_id
        ao.event("debug_cron", { "adding task", cmd_body.task_id })
        table.insert(process.crons, cmd_body)
        
    elseif command == "remove" then
        if not cmd_body.task_id or type(cmd_body.task_id) ~= "string" then
            ao.event("debug_cron", { "error", "Invalid 'remove': missing/invalid task_id", command_body = cmd_body })
            return process
        end
        -- Remove an entry by task_id
        local task_id_to_remove = cmd_body.task_id
        ao.event("debug_cron", { "removing task", task_id_to_remove, "crons_count_before", #process.crons })

        -- Find and remove the task with matching task_id
        local removed = false
        for i = #process.crons, 1, -1 do
            local job_wrapper = process.crons[i] -- This is the Erlang 'message.body' from the 'put'
            if job_wrapper and job_wrapper.body and job_wrapper.body.body and job_wrapper.body.body.task_id == task_id_to_remove then
                table.remove(process.crons, i)
                ao.event("debug_cron", { "removed task", task_id_to_remove, "crons_count_after", #process.crons })
                removed = true
                break 
            end
        end
        if not removed then
            ao.event("debug_cron", { "warn", "Task not found for removal", task_id = task_id_to_remove })
       end
        
    elseif command == "clear" then
        -- Clear all entries
        ao.event("debug_cron", { "clearing all tasks" })
        process.crons = {}

    else
        ao.event("debug_cron", { "error", "Unknown command received", command = command, command_body = cmd_body })
        return process 
    end
    
    return process
end
