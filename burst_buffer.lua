-- Copyright (C) Bull S.A.S
--

-- Add your plugin name here:
plugins = {
--	"fa_bb", -- Bull Flash Accelerators
}

-- each "plugin" is a lua module that is required to implement a get_plugin() function
-- which returns something like this table:
--
-- 	local plugin = {
-- 		job_process  = job_process_func,
-- 		job_teardown = job_teardown_func,
-- 		setup        = setup_func,
-- 		paths        = paths_func,
-- 		data_in      = data_in_func,
-- 		pre_run      = pre_run_func,
-- 		post_run     = post_run_func,
-- 		data_out     = data_out_func,
-- 		get_status   = get_status_func
-- 	}
--
-- 	All the functions declared in the plugin table take the same
-- 	parameters as the corresponding function called by slurm
-- 	(i.e for job_process, see slurm_bb_job_process()), except:
-- 	- paths() which gets a dict instead of the path_file. See comment
-- 	  in slurm_bb_paths() below.
--
-- 	The functions are supposed to return slurm.SUCCESS (and possibly a message),
-- 	or slurm.ERROR with an error message
--

---------------- No user serviceable parts below (hopefully) ------------

function build_plugins_table()
	local bb_table = {}
	for idx, plugin_name in ipairs(plugins) do
		local mod = require(plugin_name)
		table.insert(bb_table, mod.get_plugin())
	end
	return bb_table
end

function call_plugins(function_name, ...)
	local bb_plugins = build_plugins_table()
	local all_output = ""
	for i, plugin in ipairs(bb_plugins) do
		func = plugin[function_name]
		if (func) then
			local rc, msg = func(...)
			if (rc == slurm.ERROR) then
				return rc, msg
			end
			if (msg) then
				all_output = all_output .. msg
			end
		end
	end
	return slurm.SUCCESS, all_output
end

lua_script_name="burst_buffer.lua" -- for tracing purposes

-- slurm_bb_job_process (SYNCHRONOUS)
--
-- This function is called on job submission.
-- It only checks that the BB parameters are valid.
-- If this function returns an error, the job is rejected and the second return
-- value (if given) is printed where salloc, sbatch, or srun was called.
function slurm_bb_job_process(job_script, uid, gid, job_info)
	slurm.log_info("%s: slurm_bb_job_process(). job_script=%s, uid=%s, gid=%s",
		lua_script_name, job_script, uid, gid)

	return call_plugins("job_process", job_script, uid, gid, job_info)
end

-- slurm_bb_job_teardown
--
-- This function is called after the job completes or is cancelled
-- not quite sure what 'hurry' is ?
function slurm_bb_job_teardown(job_id, job_script, hurry, uid, gid)
	slurm.log_info("%s: slurm_bb_job_teardown(). job id:%s, job script:%s, hurry:%s, uid:%s, gid:%s",
		lua_script_name, job_id, job_script, hurry, uid, gid)

	return call_plugins("job_teardown", job_id, job_script, hurry, uid, gid)
end


-- slurm_bb_setup
--
-- This function is called while the job is pending and will create the burst buffers for the job
function slurm_bb_setup(job_id, uid, gid, pool, bb_size, job_script, job_info)
	slurm.log_info("%s: slurm_bb_setup(). job id:%s, uid: %s, gid:%s, pool:%s, size:%s, job script:%s",
		lua_script_name, job_id, uid, gid, pool, bb_size, job_script)

	return call_plugins("setup", job_id, uid, gid, pool, bb_size, job_script, job_info)
end

-- slurm_bb_paths (SYNCHRONOUS)
--
-- This function is called after the job is scheduled but before the
-- job starts running when the job is in a "running + configuring" state.
--
-- The file specfied by path_file is an empty file that must be filled with
-- the environment variables needed by the job
function slurm_bb_paths(job_id, job_script, path_file, uid, gid, job_info)
	slurm.log_info("%s: slurm_bb_paths(). job id:%s, job script:%s, path file:%s, uid:%s, gid:%s",
		lua_script_name, job_id, job_script, path_file, uid, gid)


	-- plugins might want to touch the same variables, for instance, $PATH
	-- So, rather than having each plugin simply writing its vars to path_file,
	-- we setup and dict. plugins that want to touch a "common" variable must check
	-- if the variable already exists and modify them, rather than simply overwriting them.
	local export_vars = {}
	local rc, output = call_plugins("paths", job_id, job_script, export_vars, uid, gid, job_info)

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
	return call_plugins("data_in", job_id, job_script, uid, gid, job_info)
end
function slurm_bb_pre_run(job_id, job_script, uid, gid, job_info)
	return call_plugins("pre_run", job_id, job_script, uid, gid, job_info)
end

function slurm_bb_post_run(job_id, job_script, uid, gid, job_info)
	return call_plugins("post_run", job_id, job_script, uid, gid, job_info)
end

function slurm_bb_data_out(job_id, job_script, uid, gid, job_info)
	return call_plugins("data_out", job_id, job_script, uid, gid, job_info)
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
	slurm.log_info("%s: slurm_bb_get_status(), uid: %s, gid:%s",
		lua_script_name, uid, gid)

	return call_plugins("get_status", uid, gid, ...)
end
