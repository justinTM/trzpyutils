# trzpyutils

## run `Py DocTest` job locally
```bash
gitlab-ci-local \
    --variable CI_PROJECT_ID=$CI_PROJECT_ID \
    --variable CI_JOB_TOKEN=$CI_JOB_TOKEN \
    --variable DOCKER_HOST=""
    --variable HOSTNAME=$(hostname) \
    --variable GITLAB_USERNAME=$GITLAB_USERNAME \
    --volume ~/.aws:/root/.aws \
    --privileged --mount-cache \
    'Py DocTest'
```

## run `pages` job locally
```bash
gitlab-ci-local \
    --variable DOCKER_HOST="" \
    --volume ~/.aws:/root/.aws \
    --privileged --mount-cache \
    'pages'
```