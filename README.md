# trzpyutils

documentation link:
https://trzpyutils-trapezoidtech-f9214799d4dd50ec23851e5ed8d318c54d0918.gitlab.io

## run `Py DocTest` job locally
note: `gitlab-ci-local` requires you to `git add <file>` if you create a new python file not already in the git index
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