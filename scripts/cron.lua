local handle_put_command
local handle_remove_command
local handle_clear_command

-- Helper functions for validation
local validate_initial_message = function(message)
    if not message or not message.body or not message.body.body then
        return false, { "error", "Initial message structure invalid", raw_message_body = message.body }
    end

    return true
end

local validate_command_body = function(cmd_body)
    if not cmd_body.path or type(cmd_body.path) ~= "string" then
        return false, { "error", "Command body missing or invalid path", received_body = cmd_body }
    end

    return true
end

-- Command handlers
local handle_put_command = function(process, cmd_body)
    if not cmd_body.task_id or type(cmd_body.task_id) ~= "string" then
        ao.event("debug_cron", { "error", "Invalid put: missing/invalid task_id", command_body = cmd_body })
        return process
    end

    if not cmd_body.data or type(cmd_body.data) ~= "table" then
        ao.event("debug_cron", { "error", "Invalid put: missing/invalid data", command_body = cmd_body })
        return process
    end

    ao.event("debug_cron", { "adding task", cmd_body.task_id })
    table.insert(process.crons, cmd_body) 
    return process
end

local handle_remove_command = function(process, cmd_body)
    if not cmd_body.task_id or type(cmd_body.task_id) ~= "string" then
        ao.event("debug_cron", { "error", "Invalid remove: missing/invalid task_id", command_body = cmd_body })
        return process
    end

    local task_id_to_remove = cmd_body.task_id
    ao.event("debug_cron", { "removing task", task_id_to_remove, "crons_count_before", #process.crons })

    -- Find the index of the task to remove.
    local idx_to_remove = nil
    for i, job in ipairs(process.crons) do
        if job.task_id == task_id_to_remove then
            idx_to_remove = i
            break
        end
    end
    
    -- If an index is found, remove the task. Otherwise, log a warning.
    if idx_to_remove then
        table.remove(process.crons, idx_to_remove)
        ao.event("debug_cron", { "removed task", task_id_to_remove, "crons_count_after", #process.crons })
    else
        ao.event("debug_cron", { "warn", "Task not found for removal", task_id = task_id_to_remove })
    end

    return process
end

local handle_clear_command = function(process)
    ao.event("debug_cron", { "clearing all tasks" })
    process.crons = nil 
    collectgarbage() 
    process.crons = {}
    return process
end

function compute(process, message, opts)
    -- Early return when no body is provided
    -- This handles the initial invocation during process setup
    -- Validation of the initial message
    local is_valid, validation_error_details = validate_initial_message(message)
    if not is_valid then
        ao.event("debug_cron", validation_error_details)
        return process
    end

    local cmd_body = message.body.body
    is_valid, validation_error_details = validate_command_body(cmd_body)
    if not is_valid then
        ao.event("debug_cron", validation_error_details)
        return process
    end

    local command = cmd_body.path
    ao.event("debug_cron", { "compute command", command })
    -- Initialize crons table if it doesn't exist
    process.crons = process.crons or {}
    -- Command processing
    if command == "put" then
        return handle_put_command(process, cmd_body)     
    elseif command == "remove" then
        return handle_remove_command(process, cmd_body)
    elseif command == "clear" then
        return handle_clear_command(process)
    else
        ao.event("debug_cron", { "error", "Unknown command received", command = command, command_body = cmd_body })
        return process 
    end
end
