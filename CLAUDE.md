# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Ozone is a scalable, redundant, and distributed object store for Hadoop and cloud-native environments. It provides S3-compatible APIs and Hadoop filesystem interfaces, supporting billions of objects with strong consistency via Apache Ratis (RAFT consensus).

## Build Commands

### Basic Build
```bash
# Full build without tests
mvn clean verify -DskipTests

# Or use install instead of verify
mvn clean install -DskipTests
```

### Build Optimizations
```bash
# Skip shaded Ozone FS jar creation (saves time if not testing Hadoop FS integration)
mvn clean install -DskipTests -DskipShade

# Skip building Recon Web UI (saves ~2 minutes)
mvn clean install -DskipTests -DskipRecon

# Build distribution tarball (like official releases)
mvn clean install -DskipTests -Pdist

# Combine optimizations for faster builds
mvn clean install -DskipTests -DskipShade -DskipRecon
```

## Testing

### Check Scripts
All test/check scripts are in `hadoop-ozone/dev-support/checks/`. Each outputs results to `target/<name>/`.

**Quick checks (< 2 minutes):**
```bash
# Code style and conventions
hadoop-ozone/dev-support/checks/checkstyle.sh
hadoop-ozone/dev-support/checks/author.sh      # Check for @author tags
hadoop-ozone/dev-support/checks/rat.sh         # Apache license headers

# Static analysis
hadoop-ozone/dev-support/checks/pmd.sh
hadoop-ozone/dev-support/checks/findbugs.sh    # SpotBugs (slower ~10 min)

# Other quick checks
hadoop-ozone/dev-support/checks/bats.sh        # Shell script unit tests
hadoop-ozone/dev-support/checks/docs.sh        # Documentation build check
hadoop-ozone/dev-support/checks/dependency.sh  # JAR dependencies check
```

**Test suites:**
```bash
# Unit tests (pure JUnit tests)
hadoop-ozone/dev-support/checks/unit.sh

# Integration tests (mini-cluster based, ~1 hour)
hadoop-ozone/dev-support/checks/integration.sh

# Acceptance tests (Docker Compose + Robot framework, ~1 hour)
hadoop-ozone/dev-support/checks/acceptance.sh

# Kubernetes tests
hadoop-ozone/dev-support/checks/kubernetes.sh
```

### Running Individual Tests
```bash
# Run a single test class
mvn test -Dtest=TestClassName

# Run a single test method
mvn test -Dtest=TestClassName#testMethodName

# Run tests in a specific module
cd hadoop-ozone/ozone-manager
mvn test
```

## Docker Development Workflow

### Quick Start with Docker Compose
After building, start a local cluster:
```bash
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone
OZONE_REPLICATION_FACTOR=3 ./run.sh -d
```

### Available Compose Configurations
Located in `hadoop-ozone/dist/src/main/compose/`:
- `ozone/` - Basic non-secure cluster
- `ozonesecure/` - Kerberos-enabled secure cluster
- `ozone-ha/` - High availability with OM HA
- `ozone-om-ha/` - OM HA specific setup
- `ozone-multiraft/` - Multi-RAFT group configuration
- `ozone-balancer/` - Includes balancer service
- `upgrade/` - Upgrade testing
- `restart/` - Restart testing

## Code Style Conventions

- **Indentation**: 2 spaces (not tabs)
- **Line length**: 120 characters max
- **License headers**: Apache 2.0 required in most files
- **No @author tags**: Use git history for authorship
- **EditorConfig**: Project uses `.editorconfig` for IDE integration
- **Checkstyle config**: `hadoop-hdds/dev-support/checkstyle/checkstyle.xml`

## Architecture Overview

### Core Components

**Ozone Manager (OM)** - `hadoop-ozone/ozone-manager/`
- Namespace manager for volumes, buckets, and keys
- Metadata persistence via RocksDB with multiple column families
- High availability via Apache Ratis (RAFT consensus)
- Block allocation requests to SCM
- ACL enforcement and multi-tenancy support
- Key classes: `OzoneManager.java`, `KeyManagerImpl.java`, `OMMetadataManager.java`

**Storage Container Manager (SCM)** - `hadoop-hdds/server-scm/`
- Block space management and cluster coordination
- Container lifecycle (containers are 5GB replication units)
- Pipeline management for RAFT replication groups
- DataNode health monitoring and commands
- Certificate Authority for security
- Replication management (under/over replication handling)
- Key classes: `StorageContainerManager.java`, `ContainerManagerImpl.java`, `PipelineManagerImpl.java`

**DataNode** - `hadoop-hdds/container-service/`
- Actual data storage using local filesystem
- Participates in RAFT replication pipelines
- Can belong to multiple pipelines simultaneously (multi-RAFT)
- Executes SCM commands (replication, deletion, etc.)

**Recon** - `hadoop-ozone/recon/`
- Management, monitoring, and aggregation service
- Maintains local copies of OM/SCM RocksDB for read-only queries
- SQL database for aggregated metrics
- Web UI and REST APIs
- Prometheus integration

**S3 Gateway** - `hadoop-ozone/s3gateway/`
- AWS S3-compatible REST API
- Bucket and object operations
- Authentication via S3 secrets

**OzoneFS** - `hadoop-ozone/ozonefs*/`
- Hadoop FileSystem-compatible interface
- Supports o3fs:// and ofs:// URI schemes

### Module Structure

**hadoop-hdds (Hadoop Distributed Data Store)**
- Foundation for distributed storage
- Protocol definitions (protobuf in `interface-*/`)
- Container services and server implementations
- Common utilities, framework, and crypto libraries

**hadoop-ozone**
- High-level object store built on HDDS
- OM, Recon, S3 Gateway implementations
- Client libraries and CLI tools
- Integration tests and mini-cluster

### Key Architectural Patterns

**RAFT Consensus (Apache Ratis)**
- OM and SCM use RAFT for HA and metadata replication
- DataNode pipelines use RAFT for container replication
- Double-buffer pattern: RAFT log → async write to RocksDB for performance
- Leader election and automatic failover

**Multi-RAFT Feature** (Current Branch: `multi-raft`)
- Distributes bucket metadata across multiple RAFT groups for scalability
- Location: `hadoop-ozone/ozone-manager/src/main/java/org/apache/hadoop/ozone/om/multiraft/`
- Key components:
  - `OmRaftGroupManager`: Assigns buckets to RAFT groups
  - `BucketRaftGroupsReconciler`: Background reconciliation service
  - Client-side caching of bucket-to-RAFT-group mappings
- Configuration: `ozone.om.multi.raft.bucket.enabled`, `ozone.om.multi.raft.bucket.groups`

**Request Processing Pattern**
- All OM requests extend `OMClientRequest`
- Flow: validation → state change → RocksDB update → response
- Separate request handlers for FSO (File System Optimized) and non-FSO modes

**Container as Replication Unit**
- Containers (not blocks) are the unit of replication (~5GB each)
- Reduces metadata overhead compared to block-level replication
- Closed containers use async replication; open containers use RAFT

**Event-Driven Background Services**
- Framework in `hadoop-hdds/framework/`
- Async processing for replication, deletion, health checks
- Decouples request handling from background maintenance

### Data Flow

**Write Path:**
1. Client → OM: Create key request
2. OM → SCM: Allocate blocks/containers
3. SCM returns block locations
4. OM records allocation in metadata
5. Client writes data to DataNode(s) in pipeline
6. Client → OM: Commit key

**Read Path:**
1. Client → OM: Get key info
2. OM returns block list with block tokens
3. Client reads directly from DataNode(s)

### Protocol Layers
- RPC/gRPC for network communication
- Protocol Buffers for serialization
- Separate protocols: Client, Server, Admin, DataNode
- Security via Kerberos, delegation tokens, block tokens, mTLS

## IntelliJ IDEA Setup

### Run Configurations
Pre-configured run configurations in `.run/` directory are auto-imported:
1. `StorageContainerManagerInit` - Initialize SCM
2. `StorageContainerManager` - Start SCM
3. `OzoneManagerInit` - Initialize OM (requires running SCM)
4. `OzoneManager` - Start OM
5. `Recon` - Start Recon (required by datanodes)
6. `Datanode1`, `Datanode2`, `Datanode3` - Start datanodes

### Common IDE Issues

**Large generated classes:**
- Add `idea.max.intellisense.filesize=10000` to IDE custom properties
- Access via Help → Edit Custom Properties
- Restart IDE

**"Bad class file" error:**
- Usually resolved by deleting the problematic class file
- Sometimes requires full Rebuild

## CI System

GitHub Actions workflows in `.github/workflows/`:
- `ci.yml` - Main CI workflow, runs on every PR
- `post-commit.yml` - Build-branch workflow
- Selective CI checks based on changed files
- Matrix builds for different Java versions (8, 11)

### Triggering CI
Enable `build-branch` workflow in your fork to run CI on your branches.

## Contributing Workflow

1. Create Jira issue in [HDDS project](https://issues.apache.org/jira/projects/HDDS/)
2. Create branch: `git checkout -b HDDS-1234`
3. Make changes, commit with logical parts
4. Push to fork and wait for `build-branch` CI to pass
5. Create PR with Jira link and testing instructions
6. Address review comments in incremental commits
7. Use `git merge --no-edit origin/master` to resolve conflicts (not rebase)
8. Avoid force-push when updating PR

## Multi-RAFT Development Notes

When working on multi-RAFT features:
- OM requests may need to route to specific RAFT groups based on bucket
- Client-side caching improves performance but requires cache invalidation logic
- Leadership balancing ensures even load distribution across RAFT groups
- Reconciliation tasks handle inconsistencies between bucket assignments
- Test with `ozone-multiraft` Docker Compose configuration

## Key Files to Know

- `pom.xml` - Root Maven configuration with all dependency versions
- `.editorconfig` - Code style settings
- `hadoop-hdds/dev-support/checkstyle/checkstyle.xml` - Checkstyle rules
- `hadoop-ozone/dist/src/main/license/jar-report.txt` - Expected JAR dependencies
- `CONTRIBUTING.md` - Detailed contribution guidelines

## Common Patterns in Code

- RocksDB column families for different metadata types (volumes, buckets, keys, etc.)
- Double-buffer writes: RAFT log entry → background thread → RocksDB
- Safe mode management during cluster startup (SCM and OM)
- Block token validation for secure data access
- Snapshot and checkpoint support for backup/recovery
- Erasure coding support as alternative to RAFT replication
