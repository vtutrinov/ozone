#!/usr/bin/python
import subprocess
import json
import os
import glob
import argparse


def get_container_report():
    output = subprocess.check_output(["ozone", "debug", "container", "list"])
    container_list_parts = output.split("\n}\n")
    list_len = len(container_list_parts)
    db_files = set()
    containers = {}
    if list_len > 0:
        container_list_parts.pop(list_len - 1)
        for i in range(0, list_len - 1):
            container_list_parts[i] = json.loads(container_list_parts[i] + "}")
            container_id = str(container_list_parts[i]["containerID"])
            containers[container_id] = container_list_parts[i]
            containers[container_id]["blocks"] = {}
            containers[container_id]["orphanedBlocks"] = list()
            db_files.add(containers[container_id]["dbFile"])
            for block_file in glob.iglob(containers[container_id]["containerPath"] + "/**/*.block"):
                containers[container_id]["orphanedBlocks"].append(block_file)

    for dbFile in db_files:
        chunks_scan_command_output = subprocess.check_output(["ozone", "debug", "ldb", "--db=" + dbFile, "scan", "--cf=" + "block_data"])
        blocks_data = json.loads(chunks_scan_command_output)
        for block_id in blocks_data:
            block_id_parts = block_id.split("|")
            container_id = block_id_parts[0]
            block_local_id = block_id_parts[1]
            containers[container_id]["blocks"][block_local_id] = blocks_data[block_id]
            containers[container_id]["blocks"][block_local_id]["blockFile"] = containers[container_id]["chunksPath"] + "/" + block_local_id + ".block"
            block_file_exists = os.path.isfile(containers[container_id]["blocks"][block_local_id]["blockFile"])
            containers[container_id]["blocks"][block_local_id]["blockFileExists"] = block_file_exists
            if block_file_exists:
                containers[container_id]["orphanedBlocks"].remove(containers[container_id]["blocks"][block_local_id]["blockFile"])

    return containers


args_parser = argparse.ArgumentParser(description="Generate datanode containers report")
args_parser.add_argument("--stdout", dest="stdout", action='store_true')
args_parser.add_argument("--report-file", dest="report_file", default="report.json")
args = args_parser.parse_args()
report = get_container_report()
if args.stdout:
    print(json.dumps(report, indent=4))
else:
    with open(args.report_file, 'w') as f:
        json.dump(report, f, indent=4)
