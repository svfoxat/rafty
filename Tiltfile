load('ext://restart_process', 'docker_build_with_restart')

local_resource('protobufs',
    cmd='make generate',
    deps=['./api/proto'],
)

local_resource(
    'build',
    cmd='make build',
    deps=[
        './cmd/server/main.go',
        './internal',
        'protobufs',
    ]
)

docker_build_with_restart(
    'rafty',
    entrypoint=['/app/server'],
    context='.',
    only=['./bin'],
    live_update=[
       sync('./bin/server', '/app/server'),
    ],
)

k8s_yaml(['./deployment/sts.yaml'])
k8s_resource('rafty', port_forwards=['12346:12346'])
