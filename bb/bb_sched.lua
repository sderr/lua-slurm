-- Copyright (C) Bull S.A.S
--


local bb_sched={}

-- A few constants:
bb_sched.POLL_PERIOD=5000 -- ms
bb_sched.CO_YIELDS = 1
bb_sched.CO_DONE = 2
bb_sched.CO_ERROR = 3

-- Run a command, synchronously
-- @param argv A table with the command and its parameters. argv[1] is the command
-- @return 2 values: command_output, exit_status
--
-- If the command was killed, exit_status is -signum
--
-- Note: This function is non-blocking and will yield when needed. In that case it will
-- yield 2 values: CO_YIELDS, { fd }
-- where fd can be polled.
--
-- Note: There is no clear error if the command fails to execute. There will simply be no
-- output and an exit status of 2.
function bb_sched.run_synchronous_command(argv)
	local popen = require("posix").popen
	local rpoll = require("posix.poll").rpoll
	local read = require("posix.unistd").read
	local wait = require("posix.sys.wait").wait

	local pipe = popen(argv, "r")
	local fd = pipe["fd"]
	local pid = pipe["pids"][1]
	local output = ""
	local N = 4096
	while true do
		-- Yield until there is data available
		while true do
			local ret, message, err = rpoll(fd, 0)
			if (ret == 1) then
				break
			else
				coroutine.yield(bb_sched.CO_YIELDS, { fd })
			end
		end
		local r = read(fd, N)
		if (r == nil or string.len(r) == 0) then
			-- End of stream
			break
		end
		output = output .. r
	end

	local epid, state, rc = wait(pid)
	if (state == "killed") then
		-- turn signum into negative rc
		rc = -rc
	end

	return output, rc
end

function bb_sched.table_empty(t)
	return next(t) == nil
end

-- Run all runnable tasks, once.
--
-- @param[in,out]   tasks_todo    The list of active tasks to run
-- @param[in,out]   tasks_done    The list of completed tasks
--
-- Both tasks_todo and tasks_done are updated each time a task completes.
-- Tasks which end are moved from tasks_todo to tasks_done.
--
-- @return list of fds to poll
--
-- A task is a table which looks like this: {
--     thread = coroutine created
--     result = after completion, the return value of the task
--     state = after completion, CO_DONE, or CO_ERROR in case of a Lua error
--     error = In case of Lua error, the error message
-- }
--
-- The tasks are stored in a table, indexed by a per-task name, that must be unique.
function bb_sched.run_tasks_once(tasks_todo, tasks_done)
	local fds = {}
	for name, task in pairs(tasks_todo) do -- don't use ipairs() here because we remove items
		local ok, val1, val2 = coroutine.resume(task["thread"])
		if (ok) then
			local done = val1
			local result = val2
			if (done == bb_sched.CO_DONE) then
				tasks_todo[name] = nil -- remove that thread from the list
				task["result"] = result
				task["state"] = bb_sched.CO_DONE
				tasks_done[name] = task
			elseif (done == bb_sched.CO_YIELDS) then
				-- When a coroutine yields, it gives us
				-- file descriptors to poll
				for _, fd in ipairs(result) do
					table.insert(fds, fd)
				end
			else
				local U = require("bb_utils")
				U.trace("SCHEDULER: bad value from %s: %s ", name, U.dump(done))
				tasks_todo[name] = nil -- remove that thread from the list
				task["state"] = bb_sched.CO_ERROR
				task["error"] = "bad value"
				tasks_done[name] = task
			end
		else
			local errmsg = val1
			tasks_todo[name] = nil -- remove that thread from the list
			task["state"] = bb_sched.CO_ERROR
			task["error"] = errmsg
			tasks_done[name] = task
		end
	end
	return fds
end

-- Add a task to a list of tasks
-- @param   tasks_todo    The list of tasks
-- @param   name          Name of the task. MUST BE UNIQUE.
-- @param   func          Function to execute
-- @param   ...           Parameters to the function
function bb_sched.add_task(tasks_todo, name, func, ...)
	if (tasks_todo[name] ~= nil) then
		error("add_task(): a task named " .. name .. " already exists")
	end
	local args = table.pack(...)
	local func_with_args = function()
		-- The function may return multiple values,
		-- pack them for storage
		return bb_sched.CO_DONE, table.pack(func(table.unpack(args)))
	end
	local task = {
		thread = coroutine.create(func_with_args)
	}
	tasks_todo[name] = task
end

-- Run a list of tasks, once, and waits...
--
-- After running the tasks, this function will block until there is again something to do.
-- This happens either:
--
--  - when one of the file descriptors of a task becomes readable (new data). For this to work,
--    tasks functions must yield(CO_YIELDS, { list of fds to poll })
--
--  - or after a periodic timeout of POLL_PERIOD ms
--
-- @param[in,out]   tasks_todo    The list of active tasks to run
-- @param[in,out]   tasks_done    The list of completed tasks
--
-- Both tasks_todo and tasks_done are updated each time a task ends.
--
-- @return true if all tasks have ended
--         false if there are still active tasks
function bb_sched.schedule_main(tasks_todo, tasks_done)
	local fds = bb_sched.run_tasks_once(tasks_todo, tasks_done)
	if (bb_sched.table_empty(tasks_todo)) then
		return true -- done
	end

	local poll = require("posix.poll").poll
	local pollin = {}
	for i,fd in ipairs(fds) do
		pollin[fd] = {events={IN=true}}
	end
	poll(pollin, bb_sched.POLL_PERIOD)

	return false  -- there's work left to do
end

-- Run a list for tasks until completion, for the main scheduler.
-- Returns the list of completed tasks
function bb_sched.run_main_tasks(tasks_todo)
	local tasks_done = {}

	repeat
	until (bb_sched.schedule_main(tasks_todo, tasks_done))

	return tasks_done
end

-- Run a list for tasks until completion, for a sub-scheduler.
-- Returns the list of completed tasks
function bb_sched.run_sub_tasks(tasks_todo)
	local tasks_done = {}

	while true do
		local fds = bb_sched.run_tasks_once(tasks_todo, tasks_done)
		if (bb_sched.table_empty(tasks_todo)) then
			return tasks_done
		end
		coroutine.yield(bb_sched.CO_YIELDS, fds)
	end
end

return bb_sched
