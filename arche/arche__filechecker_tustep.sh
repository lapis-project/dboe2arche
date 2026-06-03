#/bin/bash
echo "run filechecker for for ${TOPCOLID}"
rm -rf ${PWD}/fc_out && mkdir ${PWD}/fc_out
docker run \
  --rm \
  --network="host" \
  -v ${PWD}/fc_out_tustep:/reports \
  -v /home/csae8092/Schreibtisch/tustep/tustep01/data/120:/data \
  --entrypoint arche-filechecker \
  acdhch/arche-ingest \
  --overwrite --skipWarnings /data /reports
