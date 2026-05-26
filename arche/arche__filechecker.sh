#/bin/bash
echo "run filechecker for for ${TOPCOLID}"
rm -rf ${PWD}/fc_out && mkdir ${PWD}/fc_out
docker run \
  --rm \
  --network="host" \
  -v ${PWD}/fc_out:/reports \
  -v /home/csae8092/repos/dboe/dboe_orig_xml/:/data \
  --entrypoint arche-filechecker \
  acdhch/arche-ingest \
  --overwrite --skipWarnings /data /reports
