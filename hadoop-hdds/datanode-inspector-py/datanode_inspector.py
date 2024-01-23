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


    db_files = list(db_files)
    block_report_files = []
    for container_id in containers:
        container_db_file = containers[container_id]["dbFile"]
        blocks_report_file = container_db_file.replace("/", "_") + ".json"
        if blocks_report_file not in block_report_files:
            db_file_index = db_files.index(container_db_file)
            chunks_scan_command_output = json.loads(subprocess.check_output(["ozone", "debug", "ldb", "--db=" + container_db_file, "scan", "--cf=" + "block_data"]))
            del db_files[db_file_index]
            block_report_files.append(blocks_report_file)
        else:
            with open(blocks_report_file, "r") as fp:
                chunks_scan_command_output = json.load(fp, "encodings.utf_8")

        blocks_data = chunks_scan_command_output
        handled_blocks = []
        for block_id in blocks_data:
            block_data = blocks_data[block_id]
            container_id = str(block_data['blockID']['containerBlockID']['containerID'])
            block_local_id = str(block_data['blockID']['containerBlockID']['localID'])
            containers[container_id]["blocks"][block_local_id] = blocks_data[block_id]
            containers[container_id]["blocks"][block_local_id]["blockFile"] = containers[container_id]["chunksPath"] + "/" + block_local_id + ".block"
            block_file_exists = os.path.isfile(containers[container_id]["blocks"][block_local_id]["blockFile"])
            containers[container_id]["blocks"][block_local_id]["blockFileExists"] = block_file_exists
            if block_file_exists:
                containers[container_id]["orphanedBlocks"].remove(containers[container_id]["blocks"][block_local_id]["blockFile"])
            handled_blocks.append(block_id)
        for bid in handled_blocks:
            del blocks_data[bid]

        with open(blocks_report_file, "w") as brf:
            json.dump(blocks_data, brf, indent=4)
        block_report_files.append(blocks_report_file)

        with open("container_" + container_id + ".json", "w") as cf:
            container_object = {container_id: containers.pop(container_id)}

            json.dump(container_object, cf, indent=4)
            containers[container_id] = {}
    print containers

args_parser = argparse.ArgumentParser(description="Generate datanode containers report")
args_parser.add_argument("--stdout", dest="stdout", action='store_true')
args_parser.add_argument("--report-file", dest="report_file", default="report.json")
args = args_parser.parse_args()
report = get_container_report()
