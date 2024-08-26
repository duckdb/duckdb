#!/bin/bash

help() {
    echo "Usage: ${0} [port] [auth]"
    echo "  port    Port number for squid to lisen to (by default 3128)"
    echo "  auth    Optional string ('auth') to force user basic authentification (autherwise no authentification is required)"
    exit 0
}

port='3128'
auth='false'
log_dir="squid_logs"
conf_file="squid.conf"
pid_file='${service_name}.pid'

while [[ $# -gt 0 ]]; do
  case "${1}" in
    -h|--help)
      help
      ;;
    -p|--port)
      port="${2}"
      shift # past argument
      shift # past value
      ;;
    --auth)
      auth='true'
      conf_file="squid_auth.conf"
      pid_file='${service_name}_auth.pid'
      shift # past argument
      ;;
    --log_dir)
      log_dir="${2}"
      shift # past argument
      shift # past value
      ;;
    *)
      echo "Unknown option ${1}"
      exit 1
      ;;
  esac
done

mkdir "${log_dir}"
touch "${log_dir}/daemon.log"
chmod -R 777 "${log_dir}"

echo "http_port 127.0.0.1:${port}"                  >"${conf_file}"
echo "pid_filename ${pid_file}"                    >>"${conf_file}"

echo 'logfile_rotate 0'                            >>"${conf_file}"
echo "logfile_daemon ${log_dir}/daemon.log"        >>"${conf_file}"
echo "access_log ${log_dir}/access.log"            >>"${conf_file}"
echo "cache_log ${log_dir}/cache.log"              >>"${conf_file}"
echo "cache_store_log ${log_dir}/cache_store.log"  >>"${conf_file}"


if [[ "${auth}" == "true" ]]; then
    # User 'john' with password 'doe'
    echo 'john:$apr1$dalj9e7s$AhqY28Hvl3EcNblNJMiXa0' >squid_users

    squid_version="$(squid -v | head -n1 | grep -o 'Version [^ ]*' | cut -d ' ' -f 2)"
    if [[ "$(uname)" == "Darwin" ]]; then
        auth_basic_program="/opt/homebrew/Cellar/squid/${squid_version}/libexec/basic_ncsa_auth"
    else
        if [[ -e '/usr/lib64/squid/basic_ncsa_auth' ]]; then
            auth_basic_program="/usr/lib64/squid/basic_ncsa_auth"
        else
            auth_basic_program="/usr/lib/squid/basic_ncsa_auth"
        fi
    fi

    echo '# Add authentification options'       >>"${conf_file}"
    echo "auth_param basic program ${auth_basic_program} squid_users" >>"${conf_file}"
    echo 'auth_param basic children 3'          >>"${conf_file}"
    echo 'auth_param basic realm Squid BA'      >>"${conf_file}"
    echo 'acl auth_users proxy_auth REQUIRED'   >>"${conf_file}"
    echo 'http_access allow auth_users'         >>"${conf_file}"
    echo 'http_access deny all'                 >>"${conf_file}"
else
    echo 'http_access allow localhost'          >>"${conf_file}"
fi

exec squid -N -f "${conf_file}"
