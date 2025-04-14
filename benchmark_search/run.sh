for i in {1,}
do
echo $i
opensearch-benchmark execute-test --target-host ${HOSTS} \
     --workload-path ./query_workload.json  \
     --test-procedure semantic-query --pipeline benchmark-only  \
     --kill-running-processes \
     --on-error abort
done

# --client-options="basic_auth_user:'',basic_auth_password:'',use_ssl:true,verify_certs:false,timeout:30" \