-- Copyright (C) Bull S.A.S
--

require "bb_wordlist"
require "bb_dispatch"

-- add your plugin(s) here:
-- require "fa_bb"

lua_script_name="burst_buffer.lua"

-- slurm_bb_job_process (SYNCHRONOUS)
--
-- This function is called on job submission.
-- It only checks that the BB parameters are valid.
-- If this function returns an error, the job is rejected and the second return
-- value (if given) is printed where salloc, sbatch, or srun was called.
function slurm_bb_job_process(job_script, uid, gid, job_info)
	local contents
	slurm.log_info("%s: slurm_bb_job_process(). job_script=%s, uid=%s, gid=%s",
		lua_script_name, job_script, uid, gid)

	local wordlists = build_bbs_wordlists(get_bb_string(job_script))

	local info = {}
	info["uid"] = uid
	info["gid"] = gid

	local all_output = ""
	for idx, bb_wordlist in ipairs(wordlists) do
		local rc, msg = validate_bb(idx, info, bb_wordlist)
		if (rc == slurm.ERROR) then
			return rc, msg
		end
		if (msg) then
			all_output = all_output .. msg
		end
	end
	return slurm.SUCCESS, all_output
end

-- slurm_bb_job_teardown
--
-- This function is called after the job completes or is cancelled
-- not quite sure what 'hurry' is ?
function slurm_bb_job_teardown(job_id, job_script, hurry, uid, gid)
	slurm.log_info("%s: slurm_bb_job_teardown(). job id:%s, job script:%s, hurry:%s, uid:%s, gid:%s",
		lua_script_name, job_id, job_script, hurry, uid, gid)

	local info = {}
	info["jobid"] = job_id
	info["uid"] = uid
	info["gid"] = gid

	local wordlists = build_bbs_wordlists(get_bb_string(job_script))

	for idx, bb_wordlist in ipairs(wordlists) do
		local rc, msg = stop_bb(idx, info, bb_wordlist)
		if (rc == slurm.ERROR) then
			return rc, msg
		end
	end
	return slurm.SUCCESS
end


-- slurm_bb_setup
--
-- This function is called while the job is pending and will create the burst buffers for the job
function slurm_bb_setup(job_id, uid, gid, pool, bb_size, job_script, job_info)
	slurm.log_info("%s: slurm_bb_setup(). job id:%s, uid: %s, gid:%s, pool:%s, size:%s, job script:%s",
		lua_script_name, job_id, uid, gid, pool, bb_size, job_script)

	local info = {}
	info["jobid"] = job_id
	info["uid"] = uid
	info["gid"] = gid

	local wordlists = build_bbs_wordlists(get_bb_string(job_script))

	for idx, bb_wordlist in ipairs(wordlists) do
		local rc, msg = setup_bb(idx, info, bb_wordlist)
		if (rc == slurm.ERROR) then
			return rc, msg
		end
	end
	return slurm.SUCCESS
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

	io.output(path_file)

	local info = {}
	info["jobid"] = job_id
	info["uid"] = uid
	info["gid"] = gid

	local wordlists = build_bbs_wordlists(get_bb_string(job_script))

	for idx, bb_wordlist in ipairs(wordlists) do
		local rc, msg = export_bb_vars(idx, info, bb_wordlist)
		if (rc == slurm.ERROR) then
			return rc, msg
		end
	end
	return slurm.SUCCESS
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
	return slurm.SUCCESS
end
function slurm_bb_pre_run(job_id, job_script, uid, gid, job_info)
	return slurm.SUCCESS
end

function slurm_bb_post_run(job_id, job_script, uid, gid, job_info)
	return slurm.SUCCESS
end

function slurm_bb_data_out(job_id, job_script, uid, gid, job_info)
	return slurm.SUCCESS
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

	local i, v, args
	slurm.log_info("%s: slurm_bb_get_status(), uid: %s, gid:%s",
		lua_script_name, uid, gid)

	return slurm.SUCCESS, list_all_bbs()
end
