#!/usr/bin/env bash
# Sourced by script tests that run host scripts against a fake root.
# make_fake_host SCENARIO SCRIPT... copies host scripts and creates shared
# getent and sudo stubs. Callers install their own systemctl stubs.
make_fake_host() {
    local scenario=${1:?scenario} script
    mkdir -p "$scenario/home/swarmy/scripts" "$scenario/home/swarmy/.swarmy/release" \
        "$scenario/root/etc/systemd/system" "$scenario/root/etc/swarmy" \
        "$scenario/root/usr/local/bin" "$scenario/stubs"
        for script in "${@:2}"; do cp "scripts/$script" "$scenario/home/swarmy/scripts/"; done
    node_repo=$scenario/home/swarmy
    if [[ $(id -un) == root ]]; then
        # service_home_for deliberately hardcodes root's home.
        [[ ! -e /root/swarmy && ! -L /root/swarmy ]] || { echo '/root/swarmy already exists; cannot isolate upgrade test' >&2; exit 1; }
        ln -s "$node_repo" /root/swarmy
        root_repo_link=/root/swarmy
        node_repo=/root/swarmy
    fi
    node_root=$scenario/root
    cat > "$scenario/stubs/getent" <<'STUB'
    #!/usr/bin/env bash
    if [[ $1 == passwd && $2 == "$(id -un)" ]]; then
        printf '%s:x:1000:1000::%s:/bin/bash\n' "$2" "$TEST_NODE_HOME"
    else
        command /usr/bin/getent "$@"
    fi
STUB
    cat > "$scenario/stubs/sudo" <<'STUB'
    #!/usr/bin/env bash
    [[ $1 == -n ]] && shift
    case $1 in
        install)
            # The test runs unprivileged, so drop the owner and group a real sudo would set.
            args=()
            while (( $# )); do
                case $1 in
                    -o|-g) shift 2 ;;
                    *) args+=("$1"); shift ;;
                esac
            done
            last=$(( ${#args[@]} - 1 ))
            dest=${args[last]}
            [[ $dest == /usr/local/* || $dest == /etc/* ]] && args[last]="$TEST_NODE_ROOT$dest"
            if [[ ${args[1]} == -d ]]; then
                for ((i=3; i<=last; i++)); do
                    [[ ${args[i]} == /usr/local/* || ${args[i]} == /etc/* ]] && args[i]="$TEST_NODE_ROOT${args[i]}"
                done
            fi
            command "${args[@]}" ;;
        tee) if [[ $2 == "$TEST_NODE_ROOT"/* ]]; then command tee "$2"; else command tee "$TEST_NODE_ROOT$2"; fi ;;
        ldconfig) : ;;
        systemctl) shift; systemctl "$@" ;;
        sh)
            if [[ $* == *'node-credential issue'* ]]; then
                printf '%s\n' "$*" >> "$TEST_NODE_ROOT/issued.log"
                printf 'swn.%s.test-secret\n' "$(cat "$TEST_NODE_HOME/swarmy/.swarmy/node-id")"
            elif [[ $* == *'cat > '*node-token* ]]; then
                cat > "$SWARMY_ETC_DIR/node-token"
            else
                printf '[]\n'
            fi ;;
        rm)
            shift
            for path in "$@"; do
                [[ $path == -* ]] && continue
                [[ $path == /usr/local/* || $path == /etc/* ]] && path=$TEST_NODE_ROOT$path
                command rm -rf -- "$path"
            done ;;
        *) command "$@" ;;
    esac
STUB
    chmod +x "$scenario/stubs/getent" "$scenario/stubs/sudo"
    export TEST_NODE_HOME=$scenario/home TEST_NODE_ROOT=$node_root
    export SWARMY_SYSTEMD_DIR=$node_root/etc/systemd/system SWARMY_ETC_DIR=$node_root/etc/swarmy
}
