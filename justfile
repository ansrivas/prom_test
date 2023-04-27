set dotenv-load

docker-build push='false':
    #!/usr/bin/env bash
    set -eu -o pipefail

    IMAGE=prom-test
    REGISTRY=public.ecr.aws/zinclabs
    GIT_TAG=$(git describe --tags --abbrev=0)
    GIT_HASH=$(git rev-parse --short HEAD)

    aws ecr-public get-login-password --region us-east-1 |
        docker login --username AWS --password-stdin $REGISTRY

    DOCKER_BUILDKIT=1 docker build --tag $IMAGE:latest --file - . < deploy/build/Dockerfile
    docker tag $IMAGE:latest $REGISTRY/$IMAGE:${GIT_TAG}-${GIT_HASH}

    case {{push}} in
        true|push)
            docker push $REGISTRY/$IMAGE:${GIT_TAG}-${GIT_HASH}
            docker push $REGISTRY/$IMAGE:latest
            ;;
    esac

docker-push: (docker-build 'true')
