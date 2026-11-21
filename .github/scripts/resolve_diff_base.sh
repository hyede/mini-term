# 算出「变更行检查」(rustfmt / clippy 的变更行过滤)的对账基线,写进 $GITHUB_OUTPUT 的 sha=。
#
#   bash .github/scripts/resolve_diff_base.sh
#
# ci.yml 的 Linux 与 Windows 两个作业共用这一份(原先内联在 Linux 作业里)。
# 需要完整历史(checkout 的 fetch-depth: 0)。输入全走环境变量:
#   EVENT_NAME / PR_BASE_SHA / PUSH_BEFORE_SHA / REF_NAME / REF_TYPE / DEFAULT_BRANCH / IS_FORK
set -euo pipefail

if [ "$EVENT_NAME" = "pull_request" ]; then
  base="$PR_BASE_SHA"
elif [ "$REF_TYPE" = "tag" ]; then
  # 发版 tag:变更行检查覆盖上一个 v* tag 之后的全部提交。tag push 的
  # `before` 是全零,而 merge-base(HEAD, main) 就是 HEAD 自己,两条都给不出
  # 有意义的范围。fork 首个 tag 尝试默认分支分叉点，否则从根提交算。
  prev="$(git describe --tags --abbrev=0 --match 'v*' HEAD^ 2>/dev/null || true)"
  if [ -n "$prev" ]; then
    base="$(git rev-parse "$prev^{commit}")"
  else
    base="$(git rev-list --max-parents=0 HEAD)"
    if [ "${IS_FORK:-false}" = "true" ] && \
       git rev-parse --verify "origin/$DEFAULT_BRANCH^{commit}" >/dev/null 2>&1; then
      fork_base="$(git merge-base HEAD "origin/$DEFAULT_BRANCH" || true)"
      if [ -n "$fork_base" ] && [ "$fork_base" != "$(git rev-parse HEAD)" ]; then
        base="$fork_base"
      fi
    fi
  fi
elif [ "$REF_NAME" != "$DEFAULT_BRANCH" ]; then
  base="$(git merge-base HEAD "origin/$DEFAULT_BRANCH")"
elif [ "$EVENT_NAME" = "push" ] && \
     [ -n "$PUSH_BEFORE_SHA" ] && \
     [ "$PUSH_BEFORE_SHA" != "0000000000000000000000000000000000000000" ]; then
  base="$PUSH_BEFORE_SHA"
elif git rev-parse --verify HEAD^ >/dev/null 2>&1; then
  # workflow_dispatch on the default branch has no `before` SHA.
  # Check the selected revision's latest commit instead of treating
  # the entire repository history as new work.
  base="$(git rev-parse HEAD^)"
else
  base="$(git rev-list --max-parents=0 HEAD)"
fi
git cat-file -e "$base^{commit}"
echo "sha=$base" >> "$GITHUB_OUTPUT"
