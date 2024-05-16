function checkEventType(github, event) {
	return github.event_name === event;
}

function isDraft(github) {
	if (
		checkEventType(github, 'pull_request') &&
		github.event.pull_request.draft
	) {
		return true;
	}
	return false;
}

function checkPullRequestAction(github, action) {
	if (
		checkEventType(github, 'pull_request') &&
		github.event.action === action
	) {
		return true;
	}
	return false;
}

exports.doNotRunOnDraft = function(github) {
	const is_draft = isDraft(github);
	const is_opened = checkPullRequestAction(github, 'opened');
	const is_reopened = checkPullRequestAction(github, 'reopened');
	const is_synchronize = checkPullRequestAction(github, 'synchronize')
	const is_ready_for_review = checkPullRequestAction(github, 'ready_for_review');
	if (checkEventType(github, 'pull_request') && github.event.pull_request.base.repo.html_url === 'https://github.com/duckdb/duckdb') {
		if (is_synchronize) {
			// synchronize (push to open PR) should never trigger workflows if PR is to the upstream repo
			return false;
		}
		// PR is made to upstream repo
		if (is_draft) {
			// draft PRs should never run CI when they're made to the upstream repo
			return false;
		}
		if (is_opened) {
			return true;
		}
		if (is_reopened) {
			return true;
		}
		if (is_ready_for_review) {
			return true;
		}
		return false;
	} else {
		// PR is made to a fork, should always run
		return true;
	}
};
