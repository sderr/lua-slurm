-- Copyright (C) Bull S.A.S
--


local ex1 = {}

local ex1_core = require("ex1_core")

-- This should parse the job script and check whether this plugins
-- is requested and what it needs to do
function ex1.parse_job_script(job_script)
	-- should grep job_script for #BB_LUA EXAMPLE1 and return true only if found
	return true
end

function ex1.bb_job_process(job_script, uid, gid, job_info)
	if (ex1.parse_job_script(job_script)) then
		-- something that validates the BB parameters job_script
		return slurm.SUCCESS
	end
	return slurm.SUCCESS
end

function ex1.bb_setup(job_id, uid, gid, pool, bb_size, job_script, job_info)
	if (ex1.parse_job_script(job_script)) then
		return ex1_core.create_bb()
	end

	return slurm.SUCCESS
end

function ex1.bb_data_in(job_id, job_script, uid, gid, job_info)
	if (ex1.parse_job_script(job_script)) then
		return ex1_core.datain_bb()
	end

	return slurm.SUCCESS
end

function ex1.bb_job_teardown(job_id, job_script, hurry, uid, gid)
	if (ex1.parse_job_script(job_script)) then
		return ex1_core.stop_bb()
	end
	return slurm.SUCCESS
end

function ex1.bb_paths(job_id, job_script, export_vars_dict, uid, gid, job_info)
	-- Set env var EX1_MYVAR
	export_vars_dict["EX1_MYVAR"] = "foo"
	return slurm.SUCCESS
end

function ex1.list_bbs(uid, gid, ...)
	local str = ""
	local list = ex1_core.get_bbs()
	for id, bb in pairs(list) do
		str = str .. "EX1BB: " .. id .. ":" .. bb .."\n"
	end
	return slurm.SUCCESS, str
end


function ex1.get_plugin()
	local plugin = {
		name         = "ex1",
		job_process  = ex1.bb_job_process,
		setup        = ex1.bb_setup,
		data_in      = ex1.bb_data_in,
		job_teardown = ex1.bb_job_teardown,
		paths        = ex1.bb_paths,
		get_status   = ex1.list_bbs
	}
	return plugin
end

return ex1
