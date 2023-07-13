-- Copyright (C) Bull S.A.S
--
-- This is intended to be a global burst_buffer.lua file for the Lua Burst Buffer plugin
-- of Slurm. Its goal is to route slurm calls to several vendor-provided implementations
-- of the Lua Burst Buffer interface, so several vendors of burst buffers can coexist
-- on a single Slurm instance.
--
-- Add your plugin name here:
plugins = {
	"flash_accelerators",
	--"example1",
}

slurm_lua_root = os.getenv("SLURM_LUA_ROOT")
if (slurm_lua_root == nil) then
	slurm_lua_root = "/etc/slurm"
end
plugins_dir = "bb" -- subdirectory in slurm_lua_root where the Lua scripts are

-- Each "plugin" is a lua module.
-- The plugin "foo" must be in the directory $plugins_dir/foo and have a main file foo.lua which
-- implements some of these functions:
--
--         slurm_bb_job_process
--         slurm_bb_job_teardown
--         slurm_bb_setup
--         slurm_bb_paths
--         slurm_bb_data_in
--         slurm_bb_pre_run
--         slurm_bb_post_run
--         slurm_bb_data_out
--         slurm_bb_get_status
--
-- All these functions have the same parameters as the corresponding function called by slurm
-- except:
-- - slurm_bb_paths() which gets a dict instead of the path_file. See comment
--   in slurm_bb_paths() below.
--
-- The functions are supposed to return slurm.SUCCESS (and possibly a message),
-- or slurm.ERROR with an error message
--
-- All functions are optional.
--
-- Functions must NOT block. If they need to wait for outside events, they may call:
--
--     coroutine.yield(bb_sched.CO_YIELD, { list of fds to poll for read (possibly empty) })
--
-- bb_sched.CO_YIELD is defined in bb/bb_sched.lua
--
-- 
-- Some of these functions are synchronous (called directly from the main
-- slurmctld thread) and must return quickly. Others are asynchronous (called from a
-- separate thread in slurmscriptd) and can take longer. Note however that the burst buffer
-- Lua plugin will still impose timeouts.
--
-- Synchronous functions:
--      slurm_bb_job_process
--      slurm_bb_paths
--
-- Asynchronous functions:
--      slurm_bb_setup             (OtherTimeout, default 5 min)
--      slurm_bb_data_in           (StageInTimeout, default 1 day)
--      slurm_bb_pre_run           (OtherTimeout)
--      slurm_bb_post_run          (OtherTimeout)
--      slurm_bb_data_out          (StageOutTimeout, default 1 day)
--      slurm_bb_job_teardown      (OtherTimeout)
--      slurm_bb_get_status        (OtherTimeout)
--
--      slurm_bb_real_size (currently not used) (OtherTimeout)
--      slurm_bb_pools     (currently not used) (OtherTimeout)

package.path = slurm_lua_root .. "/bb/?.lua;" .. package.path

---------------- No user serviceable parts below (hopefully) ------------

local function load_plugin(plugin_name)
	local prev_path = package.path
	local U = require("bb_utils")
	package.path = U.safe_strcat(slurm_lua_root, "/", plugins_dir, "/", plugin_name, "/?.lua;", package.path)
	local mod = require(plugin_name)
	package.path = prev_path
	return mod
end

local function call_plugins(function_name, ...)
	local bb_utils = require("bb_utils")
	local sched = require("bb_sched")
	local tasks_todo = {}
	for _, plugin_name in ipairs(plugins) do
		local ok, plugin = pcall(load_plugin, plugin_name)
		if (not ok) then
			local err = plugin -- return value from pcall
			local errmsg = string.format("failed to load plugin %s -- check system configuration",
				plugin_name)
			slurm.log_error("lua/%s: %s : %s",
				function_name, errmsg, err)
			return slurm.ERROR, errmsg
		end
		local func = plugin[function_name]
		if (func) then
			sched.add_task(tasks_todo, plugin_name, func, ...)
		end
	end

	local tasks_done = sched.run_main_tasks(tasks_todo)

	local all_output = ""
	local all_errors = ""
	local failures = 0
	for name, task in pairs(tasks_done) do
		-- first check any possible lua runtime error in that task
		local state = task["state"]
		if (state == sched.CO_ERROR) then
			local errmsg = task["error"]
			slurm.log_error("lua/%s: task %s failed: %s",
				function_name, name, errmsg)
			all_errors = bb_utils.append_str_output(all_errors, errmsg)
			failures = failures + 1
		else
			-- unpack the task result and add it to the global output
			local result = task["result"]
			local rc, msg = table.unpack(result)
			if (rc == slurm.ERROR) then
				all_errors = bb_utils.append_str_output(all_errors, msg)
				failures = failures + 1
			elseif (rc == slurm.SUCCESS) then
				all_output = bb_utils.append_str_output(all_output, msg)
			else
				local errmsg = string.format("lua/%s: task %s returned %s instead of slurm.SUCCESS or slurm.ERROR",
					function_name, name, tostring(rc))
				slurm.log_error(errmsg)
				all_errors = bb_utils.append_str_output(all_errors, errmsg)
				failures = failures + 1
			end
		end
	end

	if (failures > 0) then
		return slurm.ERROR, all_errors
	else
		return slurm.SUCCESS, all_output
	end
end

lua_script_name="burst_buffer.lua" -- for tracing purposes

-- safe version of slurm.log_info which accepts nil values
-- this is useful when running with slurm 22 which does not pass all parameters
function safe_log_info(format, ...)
	local args = table.pack(...)
	local n = args["n"]
	for i=1,n do
		if (args[i] == nil) then
			args[i] = "nil"
		end
	end
	slurm.log_info(format, table.unpack(args))
end

-- slurm_bb_job_process (SYNCHRONOUS)
--
-- This function is called on job submission.
-- It only checks that the BB parameters are valid.
-- If this function returns an error, the job is rejected and the second return
-- value (if given) is printed where salloc, sbatch, or srun was called.
function slurm_bb_job_process(job_script, uid, gid, job_info)
	safe_log_info("%s: slurm_bb_job_process(). job_script=%s, uid=%s, gid=%s",
		lua_script_name, job_script, uid, gid)

	return call_plugins("bb_job_process", job_script, uid, gid, job_info)
end

-- slurm_bb_job_teardown
--
-- This function is called after the job completes, is cancelled, or after errors in
-- other burst buffer functions.
-- The parameter 'hurry' will be set to true if the job was not able to complete properly
function slurm_bb_job_teardown(job_id, job_script, hurry, uid, gid)
	safe_log_info("%s: slurm_bb_job_teardown(). job id:%s, job script:%s, hurry:%s, uid:%s, gid:%s",
		lua_script_name, job_id, job_script, hurry, uid, gid)

	return call_plugins("bb_job_teardown", job_id, job_script, hurry, uid, gid)
end


-- slurm_bb_setup
--
-- This function is called while the job is pending and will create the burst buffers for the job
function slurm_bb_setup(job_id, uid, gid, pool, bb_size, job_script, job_info)
	safe_log_info("%s: slurm_bb_setup(). job id:%s, uid: %s, gid:%s, pool:%s, size:%s, job script:%s",
		lua_script_name, job_id, uid, gid, pool, bb_size, job_script)

	return call_plugins("bb_setup", job_id, uid, gid, pool, bb_size, job_script, job_info)
end

-- slurm_bb_paths (SYNCHRONOUS)
--
-- This function is called after the job is scheduled but before the
-- job starts running when the job is in a "running + configuring" state.
--
-- The file specified by path_file is an empty file that must be filled with
-- the environment variables needed by the job
function slurm_bb_paths(job_id, job_script, path_file, uid, gid, job_info)
	safe_log_info("%s: slurm_bb_paths(). job id:%s, job script:%s, path file:%s, uid:%s, gid:%s",
		lua_script_name, job_id, job_script, path_file, uid, gid)


	-- plugins might want to touch the same variables, for instance, $PATH
	-- So, rather than having each plugin simply writing its vars to path_file,
	-- we setup a dict. plugins that want to touch a "common" variable must check
	-- if the variable already exists and modify it, rather than simply overwriting it.
	local export_vars = {}
	local rc, output = call_plugins("bb_paths", job_id, job_script, export_vars, uid, gid, job_info)

	if (rc == slurm.SUCCESS) then
		io.output(path_file)
		for k,v in pairs(export_vars) do
			local str = string.format("%s=%s\n", k, v)
			io.write(str)
		end
	end

	return rc, output
end


-- We don't use pools.
function slurm_bb_pools()
	return slurm.SUCCESS
end

-- We don't use capacity nor pools, this one should not be called.
function slurm_bb_real_size(job_id, uid, gid, job_info)
	return slurm.ERROR, "Unexpected call to bb_real_size()"
end

function slurm_bb_data_in(job_id, job_script, uid, gid, job_info)
	return call_plugins("bb_data_in", job_id, job_script, uid, gid, job_info)
end
function slurm_bb_pre_run(job_id, job_script, uid, gid, job_info)
	return call_plugins("bb_pre_run", job_id, job_script, uid, gid, job_info)
end

function slurm_bb_post_run(job_id, job_script, uid, gid, job_info)
	return call_plugins("bb_post_run", job_id, job_script, uid, gid, job_info)
end

function slurm_bb_data_out(job_id, job_script, uid, gid, job_info)
	return call_plugins("bb_data_out", job_id, job_script, uid, gid, job_info)
end

-- slurm_bb_get_status
--
-- This function is called when "scontrol show bbstat" is run. It receives the
-- authenticated user id and group id of the caller, as well as a variable
-- number of arguments - whatever arguments are after "bbstat".
--
-- If this function returns slurm.SUCCESS, then this function's second return
-- value will be printed where the scontrol command was run. If this function
-- returns slurm.ERROR, then this function's second return value is ignored and
-- an error message will be printed instead.
function slurm_bb_get_status(uid, gid, ...)
	safe_log_info("%s: slurm_bb_get_status(), uid: %s, gid:%s",
		lua_script_name, uid, gid)

	return call_plugins("bb_get_status", uid, gid, ...)
end
