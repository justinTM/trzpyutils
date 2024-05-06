# trzpyutils

documentation link:
https://trzpyutils-trapezoidtech-f9214799d4dd50ec23851e5ed8d318c54d0918.gitlab.io

## run `Py DocTest` job locally
note: `gitlab-ci-local` requires you to git stage files, or they won't show up inside the job.
```bash
devbox run tests
```
or
```bash
gitlab-ci-local \
    --variable DOCKER_HOST="" \
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