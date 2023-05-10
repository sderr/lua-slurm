-- Copyright (C) Bull S.A.S
--


local ex1_core = {}

function ex1_core.run_command(command_and_args)
	local sched = require("bb_sched")
	local U = require("bb_utils")
	U.trace("ex1/run_command: %s", U.dump(command_and_args))

	return sched.run_synchronous_command(command_and_args)
end

-- pretend to check if a BB is ready.
function ex1_core.bb_ready(start_time)
	local now = os.time()
	return (now - start_time >= 10)
end

-- This is an example of a simple asynchronous
-- function, which starts something and then will
-- periodically check if it's done. After yielding,
-- this function will resume every time the global scheduler
-- wakes up, so if the checking function is expensive it might be necessary
-- to ratelimit it.
function ex1_core.create_bb()
	local sched = require("bb_sched")
	local U = require("bb_utils")
	local start_time = os.time()
	while not ex1_core.bb_ready(start_time) do
		U.trace("ex1/create_bb: not ready yet")
		coroutine.yield(sched.CO_YIELDS, {})
	end
	return slurm.SUCCESS
end

-- This is an example of a simple function that calls an external
-- command synchronously. sched.run_synchronous_command()
-- takes care of all coroutine-related stuff.
function ex1_core.datain_bb()
	local U = require("bb_utils")
	local out, status = ex1_core.run_command({ "ping", "-c", "5", "localhost"})
	U.trace("ex1/datain_bb: command output %s, exit code %d", out, status)
	return slurm.SUCCESS
end


-- stop_A and stop_B are functions called by the stop example below.
-- In this example they both return a string and an integer
function ex1_core.stop_A(arg)
	local U = require("bb_utils")
	local out, status = ex1_core.run_command({ "ping", "-c", arg, "localhost"})
	U.trace("ex1/stop_A: command output %s, exit code %d", out, status)
	return "foo", 42
end

function ex1_core.stop_B(arg)
	local U = require("bb_utils")
	local out, status = ex1_core.run_command({ "ping", "-c", arg, "localhost"})
	U.trace("ex1/stop_B: command output %s, exit code %d", out, status)
	return "bar", 37
end


-- This is a more complicated example, of a plugin function which needs to do several
-- things in parallel. It basically uses the same functions as the global scheduler,
-- except it must not wait or poll itself. It must define several tasks, and
-- use sched.run_sub_tasks() to run them.
function ex1_core.stop_bb()
	local objectA = { "hello" }
	local objectB = { "world" }
	local tasks_todo = {}

	local sched = require("bb_sched")
	-- each tasks takes an "owner", which can be anything you want, a
	-- name, which must be unique in this list, a function to run,
	-- and the parameters to that function
	sched.add_task(tasks_todo, objectA, "stop_A", ex1_core.stop_A, "4")
	sched.add_task(tasks_todo, objectB, "stop_B", ex1_core.stop_B, "6")

	local tasks_done = sched.run_sub_tasks(tasks_todo)

	local U = require("bb_utils")
	U.trace("ex1/stop: tasks_done = %s", U.dump(tasks_done))

	for name, task in pairs(tasks_done) do
		-- first check any possible lua runtime error in that task
		local state = task["state"]
		if (state == sched.CO_ERROR) then
			local errmsg = task["error"]
			slurm.log_error("lua/%s: task %s failed: %s",
				function_name, name, errmsg)
			return slurm.ERROR, errmsg
		end

		-- unpack the task result and add it to the global output
		local result = task["result"]
		local s, i = table.unpack(result)
		U.trace("ex1/stop: task %s returned %s, %d", name, s, i)
	end
	return slurm.SUCCESS
end

-- return a fake list of BBs for bbstat
function ex1_core.get_bbs()
	local bbs = {}
	bbs[1] = "exbb1"
	bbs[2] = "exbb2"
	return bbs
end

return ex1_core
