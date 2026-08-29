package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/labring-sigs/pvc-migrate/internal/domain"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const mongoDBNativeSwitchoverPreflight = `
command -v sh >/dev/null
command -v mongosh >/dev/null || command -v mongo >/dev/null
username=${MONGODB_ROOT_USER:-${MONGODB_USER:-}}
password=${MONGODB_ROOT_PASSWORD:-${MONGODB_PASSWORD:-}}
test -n "$username"
test -n "$password"
`

const mongoDBNativeSwitchoverStepdownMarker = "PVC_MIGRATE_MONGODB_STEPDOWN_STARTED"

// Resolve the selected candidate from the live replica-set configuration.
// Other electable secondaries are frozen temporarily so the selected member is
// the only election target. Freeze state expires automatically and does not
// modify the durable replica-set configuration.
const mongoDBNativeSwitchoverScript = `
leader=$1
candidate=$2
case "$leader" in
  *[!a-z0-9.-]*) echo "invalid MongoDB Pod name" >&2; exit 1 ;;
esac
case "$candidate" in
  *[!a-z0-9.-]*) echo "invalid MongoDB Pod name" >&2; exit 1 ;;
esac

client=$(command -v mongosh || command -v mongo)
username=${MONGODB_ROOT_USER:-${MONGODB_USER:-}}
password=${MONGODB_ROOT_PASSWORD:-${MONGODB_PASSWORD:-}}
test -n "$username"
test -n "$password"

topology=$("$client" --quiet \
  --eval "conf=rs.config();status=rs.status();const matches=conf.members.filter(member => member.host === '$candidate' || member.host.startsWith('$candidate.') || member.host.startsWith('$candidate:'));if(matches.length !== 1){throw new Error('expected exactly one candidate member, found '+matches.length)};const target=matches[0];const priority=target.priority === undefined ? 1 : target.priority;const votes=target.votes === undefined ? 1 : target.votes;if(target.arbiterOnly || target.hidden || votes === 0 || priority <= 0){throw new Error('candidate is not electable')};const targetStatus=status.members.find(member => member.name === target.host);if(!targetStatus || targetStatus.health !== 1 || targetStatus.stateStr !== 'SECONDARY'){throw new Error('candidate is not a healthy secondary')};const primary=status.members.find(member => member.stateStr === 'PRIMARY');if(!primary){throw new Error('replica set has no primary')};if(primary.optimeDate && targetStatus.optimeDate && primary.optimeDate-targetStatus.optimeDate > 10000){throw new Error('candidate is more than 10 seconds behind the primary')};print('CANDIDATE='+target.host);conf.members.forEach(member => {const memberPriority=member.priority === undefined ? 1 : member.priority;const memberVotes=member.votes === undefined ? 1 : member.votes;if(member.host !== target.host && member.host !== primary.name && !member.arbiterOnly && !member.hidden && memberVotes > 0 && memberPriority > 0){print('FREEZE='+member.host)}})" \
  "mongodb://$leader:27017" \
  --authenticationDatabase admin \
  --username "$username" \
  --password "$password")

candidate_host=
freeze_hosts=
while IFS= read -r topology_line; do
  case "$topology_line" in
    CANDIDATE=*) candidate_host=${topology_line#CANDIDATE=} ;;
    FREEZE=*) freeze_hosts="$freeze_hosts ${topology_line#FREEZE=}" ;;
  esac
done <<EOF
$topology
EOF
test -n "$candidate_host"

for member_host in $freeze_hosts; do
  case "$member_host" in
    *[!a-zA-Z0-9._:-]*) echo "invalid MongoDB member host" >&2; exit 1 ;;
  esac

  "$client" --quiet \
    --eval "result=db.adminCommand({replSetFreeze:120});if(!result.ok){throw new Error('freeze failed')}" \
    "mongodb://$member_host" \
    --authenticationDatabase admin \
    --username "$username" \
    --password "$password"
done

echo PVC_MIGRATE_MONGODB_STEPDOWN_STARTED
"$client" --quiet \
  --eval "result=db.adminCommand({replSetStepDown:60,secondaryCatchUpPeriodSecs:15});if(!result.ok){throw new Error('primary step-down failed')}" \
  "mongodb://$leader:27017" \
  --authenticationDatabase admin \
  --username "$username" \
  --password "$password"
`

func mongoDBNativeSwitchoverArgs(leader, candidate string) []string {
	return []string{
		"sh",
		"-ceu",
		mongoDBNativeSwitchoverScript,
		"pvc-migrate-mongodb-switchover",
		leader,
		candidate,
	}
}

func (m *Manager) runMongoDBNativeSwitchover(ctx context.Context, session *domain.Session) error {
	kb := session.Spec.Workload().KubeBlocks
	if kb == nil {
		return domain.NewError(
			domain.ErrorInternal,
			"pause KubeBlocks",
			"session lacks KubeBlocks state",
		)
	}

	if kb.SwitchoverContainer == "" {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause KubeBlocks",
			"MongoDB native switchover session lacks the validated container",
		)
	}

	if m.commandExecutor == nil {
		return domain.NewError(
			domain.ErrorPrecondition,
			"pause KubeBlocks",
			"Pod exec is unavailable for the MongoDB native switchover; manual MongoDB switchover: "+kubeBlocksMongoDBNativeSwitchoverCommand(
				session.Spec.Workload().Pod.Namespace,
				kb.Cluster,
				kb.Component,
				kb.Instance,
				kb.SwitchoverCandidate,
			),
		)
	}

	namespace := session.Spec.Workload().Pod.Namespace

	selected, err := m.typed.CoreV1().Pods(namespace).Get(ctx, kb.Instance, metav1.GetOptions{})
	if err != nil {
		return domain.WrapError(
			domain.ErrorKubernetes,
			"pause KubeBlocks",
			"read MongoDB switchover source Pod",
			err,
		)
	}

	if selected.UID != session.Spec.Workload().Pod.UID {
		return domain.NewError(
			domain.ErrorConflict,
			"pause KubeBlocks",
			fmt.Sprintf("Pod %s/%s UID changed", selected.Namespace, selected.Name),
		)
	}

	if err := validatePodController(
		selected,
		session.Spec.Workload().Controller,
		"pause KubeBlocks",
	); err != nil {
		return err
	}

	const leaderAddress = "127.0.0.1"

	if m.logger != nil {
		m.logger.Info(
			"starting KubeBlocks MongoDB native switchover",
			"namespace",
			namespace,
			"cluster",
			kb.Cluster,
			"workload_component",
			kb.Component,
			"instance",
			kb.Instance,
			"candidate",
			kb.SwitchoverCandidate,
		)
	}

	result, err := m.commandExecutor.Execute(ctx, podCommandRequest{
		Namespace: namespace,
		Pod:       kb.Instance,
		Container: kb.SwitchoverContainer,
		Command:   mongoDBNativeSwitchoverArgs(leaderAddress, kb.SwitchoverCandidate),
	})

	stepdownStarted := strings.Contains(result.Stdout, mongoDBNativeSwitchoverStepdownMarker) ||
		strings.Contains(result.Stderr, mongoDBNativeSwitchoverStepdownMarker)
	if err != nil && !stepdownStarted {
		executionErr := podCommandError("run MongoDB native candidate switchover", result, err)

		return domain.WrapError(
			domain.ErrorPrecondition,
			"pause KubeBlocks",
			fmt.Sprintf(
				"%v; manual MongoDB switchover: %s",
				executionErr,
				kubeBlocksMongoDBNativeSwitchoverCommand(
					namespace,
					kb.Cluster,
					kb.Component,
					kb.Instance,
					kb.SwitchoverCandidate,
				),
			),
			executionErr,
		)
	}

	waitErr := m.waitFor(
		ctx,
		fmt.Sprintf(
			"KubeBlocks MongoDB switchover from %s to %s",
			kb.Instance,
			kb.SwitchoverCandidate,
		),
		func(waitCtx context.Context) (bool, error) {
			leader, leaderErr := m.typed.CoreV1().
				Pods(namespace).
				Get(waitCtx, kb.Instance, metav1.GetOptions{})
			if leaderErr != nil {
				return false, leaderErr
			}

			if err := validatePodController(
				leader,
				session.Spec.Workload().Controller,
				"pause KubeBlocks",
			); err != nil {
				return false, err
			}

			candidate, candidateErr := m.typed.CoreV1().
				Pods(namespace).
				Get(waitCtx, kb.SwitchoverCandidate, metav1.GetOptions{})
			if candidateErr != nil {
				return false, candidateErr
			}

			if err := validatePodController(
				candidate,
				session.Spec.Workload().Controller,
				"pause KubeBlocks",
			); err != nil {
				return false, err
			}

			return !isLeaderRole(podRole(leader)) && isLeaderRole(podRole(candidate)), nil
		},
	)
	if waitErr == nil {
		return nil
	}

	if err == nil {
		return waitErr
	}

	executionErr := podCommandError("run MongoDB native candidate switchover", result, err)
	combined := errors.Join(executionErr, waitErr)

	return domain.WrapError(
		domain.ErrorPrecondition,
		"pause KubeBlocks",
		fmt.Sprintf(
			"%v; manual MongoDB switchover: %s",
			combined,
			kubeBlocksMongoDBNativeSwitchoverCommand(
				namespace,
				kb.Cluster,
				kb.Component,
				kb.Instance,
				kb.SwitchoverCandidate,
			),
		),
		combined,
	)
}

func kubeBlocksMongoDBNativeSwitchoverCommand(
	namespace, _, _, selected, candidate string,
) string {
	args := mongoDBNativeSwitchoverArgs("127.0.0.1", candidate)

	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, posixShellQuote(arg))
	}

	return fmt.Sprintf(
		"kubectl --namespace %s exec %s -c mongodb -- %s",
		posixShellQuote(namespace),
		posixShellQuote(selected),
		strings.Join(quoted, " "),
	)
}

func posixShellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}
