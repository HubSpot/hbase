#!/usr/bin/env bash
#
# branch-report.sh — Classify HubSpot commits on top of an upstream HBase release.
#
# Usage:
#   ./dev-support/hubspot/branch-report.sh [--upgrade-to <version>] [--base-tag <tag>] [--no-color]
#
# Options:
#   --upgrade-to <ver>  Compare against a target version for upgrade planning
#   --base-tag <tag>    Override the auto-detected upstream base tag
#   --color             Force colored output (default when stdout is a terminal)
#   --no-color          Disable colored output
#
# Prerequisites:
#   Run 'git fetch upstream --tags' to ensure upstream release tags are available.
#
# Examples:
#   ./dev-support/hubspot/branch-report.sh
#   ./dev-support/hubspot/branch-report.sh --upgrade-to 2.6.6
#   ./dev-support/hubspot/branch-report.sh --base-tag rel/2.6.3 --upgrade-to 2.6.7
#
# The script:
#   1. Determines the upstream release tag that the current branch is based on.
#   2. Enumerates every commit between that tag and HEAD.
#   3. Classifies each commit as "HubSpot Edit", "HubSpot Backport", or "Unknown".
#   4. For backports that reference a JIRA ticket and an expected upstream release,
#      checks whether that ticket actually landed in that release's tag.
#   5. Prints a report with per-commit classification, warnings, and a summary.
#   6. If --upgrade-to is given, recommends which backports are still needed after
#      upgrading.
#
set -euo pipefail

# ── Colors (enabled by default when stdout is a terminal, disable with --no-color) ──
USE_COLOR=0
if [[ -t 1 ]]; then
  USE_COLOR=1
fi

c_reset=""
c_red=""
c_green=""
c_yellow=""
c_cyan=""
c_bold=""
c_dim=""

enable_color() {
  c_reset=$'\033[0m'
  c_red=$'\033[0;31m'
  c_green=$'\033[0;32m'
  c_yellow=$'\033[0;33m'
  c_cyan=$'\033[0;36m'
  c_bold=$'\033[1m'
  c_dim=$'\033[2m'
}

# ── Argument parsing ───────────────────────────────────────────────────────
UPGRADE_TO=""
BASE_TAG=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --upgrade-to)
      UPGRADE_TO="$2"; shift 2 ;;
    --base-tag)
      BASE_TAG="$2"; shift 2 ;;
    --color)
      USE_COLOR=1; shift ;;
    --no-color)
      USE_COLOR=0; shift ;;
    -h|--help)
      sed -n '2,/^$/s/^#//p' "$0"; exit 0 ;;
    *)
      echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [[ "$USE_COLOR" -eq 1 ]]; then
  enable_color
fi

# ── Determine base tag ─────────────────────────────────────────────────────
if [[ -z "$BASE_TAG" ]]; then
  # Walk back through the log to find the most recent upstream release tag
  # reachable from HEAD. Upstream release tags match rel/X.Y.Z.
  BASE_TAG=$(git describe --tags --match 'rel/*' --abbrev=0 HEAD 2>/dev/null || true)
  if [[ -z "$BASE_TAG" ]]; then
    echo "ERROR: Could not find an upstream release tag (rel/*) in HEAD's ancestry." >&2
    echo "       Use --base-tag to specify one manually." >&2
    exit 1
  fi
fi

echo "${c_bold}Branch Report${c_reset}"
echo "============="
echo ""
echo "  Current branch:  $(git branch --show-current)"
echo "  Base tag:         ${BASE_TAG}"
echo "  HEAD:             $(git rev-parse --short HEAD)"

COMMIT_COUNT=$(git rev-list "${BASE_TAG}..HEAD" | wc -l | tr -d ' ')
echo "  Commits on top:   ${COMMIT_COUNT}"
echo ""

if [[ "$COMMIT_COUNT" -eq 0 ]]; then
  echo "No commits on top of ${BASE_TAG}. Nothing to report."
  exit 0
fi



# ── Helper: check if a JIRA ticket appears in the log of a given tag ──────
# Returns 0 if found, 1 if not found, 2 if the tag doesn't exist.
ticket_in_tag() {
  local ticket="$1"
  local tag="$2"

  if ! git rev-parse --verify "refs/tags/${tag}" &>/dev/null; then
    return 2  # tag does not exist
  fi

  if git log --oneline "${tag}" --grep="${ticket}" 2>/dev/null | grep -q "${ticket}"; then
    return 0
  else
    return 1
  fi
}

# More targeted: check if ticket is in commits unique to a specific release
# i.e., commits in rel/X.Y.Z but not in the prior release
ticket_in_release() {
  local ticket="$1"
  local version="$2"
  local tag="rel/${version}"
  local major minor patch prior_tag

  if ! git rev-parse --verify "refs/tags/${tag}" &>/dev/null; then
    return 2  # tag does not exist
  fi

  # Compute the prior tag to narrow the search
  major=$(echo "$version" | cut -d. -f1)
  minor=$(echo "$version" | cut -d. -f2)
  patch=$(echo "$version" | cut -d. -f3)
  patch="${patch:-0}"

  if [[ "$patch" -gt 0 ]]; then
    prior_tag="rel/${major}.${minor}.$((patch - 1))"
  else
    # For X.Y.0, just search the whole tag history
    prior_tag=""
  fi

  local search_range
  if [[ -n "$prior_tag" ]] && git rev-parse --verify "refs/tags/${prior_tag}" &>/dev/null; then
    search_range="${prior_tag}..${tag}"
  else
    search_range="${tag}"
  fi

  if git log --oneline "${search_range}" --grep="${ticket}" 2>/dev/null | grep -q "${ticket}"; then
    return 0
  else
    return 1
  fi
}

# ── Helper: check if ticket is in the upgrade target (cumulative) ─────────
ticket_in_upgrade_target() {
  local ticket="$1"
  local target_version="$2"
  local target_tag="rel/${target_version}"

  if ! git rev-parse --verify "refs/tags/${target_tag}" &>/dev/null; then
    # Tag doesn't exist yet, check branch-2.X instead
    local major minor
    major=$(echo "$target_version" | cut -d. -f1)
    minor=$(echo "$target_version" | cut -d. -f2)
    local branch="upstream/branch-${major}.${minor}"
    if git rev-parse --verify "${branch}" &>/dev/null; then
      if git log --oneline "${branch}" --grep="${ticket}" 2>/dev/null | grep -q "${ticket}"; then
        return 0
      else
        return 1
      fi
    fi
    return 2  # can't determine
  fi

  # Check if ticket appears anywhere up to the target tag
  if git log --oneline "${target_tag}" --grep="${ticket}" 2>/dev/null | grep -q "${ticket}"; then
    return 0
  else
    return 1
  fi
}

# ── Classify commits ──────────────────────────────────────────────────────
# Arrays to hold results
declare -a commit_hashes=()
declare -a commit_subjects=()
declare -a commit_types=()       # EDIT, BACKPORT, UNKNOWN
declare -a commit_tickets=()     # HBASE-NNNNN or empty
declare -a commit_expected=()    # expected version or empty
declare -a commit_warnings=()    # warning messages

idx=0
while IFS= read -r line; do
  hash="${line%% *}"
  subject="${line#* }"

  commit_hashes+=("$hash")
  commit_subjects+=("$subject")
  commit_tickets+=("")
  commit_expected+=("")
  commit_warnings+=("")

  # Extract JIRA ticket if present
  ticket=""
  if [[ "$subject" =~ (HBASE-[0-9]+) ]]; then
    ticket="${BASH_REMATCH[1]}"
    commit_tickets[$idx]="$ticket"
  fi

  # Classify
  subject_lower=$(echo "$subject" | tr '[:upper:]' '[:lower:]')

  if [[ "$subject" =~ ^"HubSpot Edit" ]]; then
    commit_types+=("EDIT")
  elif [[ "$subject" =~ ^"HubSpot Backport" ]]; then
    commit_types+=("BACKPORT")
    if [[ -z "$ticket" ]]; then
      commit_warnings[$idx]="BACKPORT without JIRA ticket"
    fi
  elif [[ "$subject_lower" =~ "not yet upstream" ]] || \
       [[ "$subject_lower" =~ "not yet merged upstream" ]] || \
       [[ "$subject_lower" =~ "not yet written upstream" ]] || \
       [[ "$subject_lower" =~ "not yet proposed upstream" ]] || \
       [[ "$subject_lower" =~ "not yet started upstream" ]] || \
       [[ "$subject_lower" =~ "not upstreamed yet" ]] || \
       [[ "$subject_lower" =~ "do not upstream" ]]; then
    # Has upstream status annotation but no standard prefix
    if [[ -n "$ticket" ]]; then
      commit_types+=("BACKPORT")
      commit_warnings[$idx]="Missing 'HubSpot Backport:' prefix"
    else
      commit_types+=("EDIT")
      commit_warnings[$idx]="Missing 'HubSpot Edit:' prefix"
    fi
  elif [[ -n "$ticket" ]] && [[ "$subject" =~ ^"HBASE-" ]]; then
    # Starts with HBASE-NNNNN but no HubSpot prefix — likely an improperly tagged backport
    commit_types+=("BACKPORT")
    commit_warnings[$idx]="Missing 'HubSpot Backport:' prefix"
  else
    commit_types+=("UNKNOWN")
  fi

  # Extract expected version
  if [[ "$subject" =~ \(will\ be\ in\ ([0-9]+\.[0-9]+(\.[0-9]+)?)\) ]]; then
    commit_expected[$idx]="${BASH_REMATCH[1]}"
  elif [[ "$subject" =~ \(drop\ in\ ([0-9]+\.[0-9]+(\.[0-9]+)?)\) ]]; then
    commit_expected[$idx]="DROP:${BASH_REMATCH[1]}"
  fi

  idx=$((idx + 1))
done < <(git log --oneline --reverse "${BASE_TAG}..HEAD")

# ── Verify backport tickets against upstream tags ──────────────────────────
declare -a commit_landed=()      # LANDED, NOT_LANDED, NO_TAG, N/A, DROP

for ((i=0; i<${#commit_hashes[@]}; i++)); do
  ticket="${commit_tickets[$i]}"
  expected="${commit_expected[$i]}"
  ctype="${commit_types[$i]}"

  if [[ "$ctype" != "BACKPORT" ]] || [[ -z "$ticket" ]]; then
    commit_landed+=("N/A")
    continue
  fi

  if [[ -z "$expected" ]]; then
    commit_landed+=("N/A")
    continue
  fi

  if [[ "$expected" == DROP:* ]]; then
    commit_landed+=("DROP")
    continue
  fi

  rc=0
  ticket_in_release "$ticket" "$expected" || rc=$?
  if [[ $rc -eq 0 ]]; then
    commit_landed+=("LANDED")
  elif [[ $rc -eq 1 ]]; then
    commit_landed+=("NOT_LANDED")
  else
    commit_landed+=("NO_TAG")
  fi
done

# ── If upgrade target specified, check which backports are still needed ───
declare -a commit_needed_after_upgrade=()

if [[ -n "$UPGRADE_TO" ]]; then
  echo "${c_dim}Checking backport coverage in upgrade target ${UPGRADE_TO}...${c_reset}"
  for ((i=0; i<${#commit_hashes[@]}; i++)); do
    ticket="${commit_tickets[$i]}"
    ctype="${commit_types[$i]}"
    expected="${commit_expected[$i]}"

    if [[ "$ctype" == "EDIT" ]]; then
      commit_needed_after_upgrade+=("YES")
      continue
    fi

    if [[ "$ctype" == "UNKNOWN" ]]; then
      commit_needed_after_upgrade+=("REVIEW")
      continue
    fi

    # BACKPORT
    if [[ "$expected" == DROP:* ]]; then
      drop_version="${expected#DROP:}"
      # Compare versions: if upgrade target >= drop version, we can drop it
      if printf '%s\n%s\n' "$drop_version" "$UPGRADE_TO" | sort -V | head -1 | grep -q "^${drop_version}$"; then
        commit_needed_after_upgrade+=("DROP")
      else
        commit_needed_after_upgrade+=("YES")
      fi
      continue
    fi

    if [[ -z "$ticket" ]]; then
      commit_needed_after_upgrade+=("REVIEW")
      continue
    fi

    rc=0
    ticket_in_upgrade_target "$ticket" "$UPGRADE_TO" || rc=$?
    if [[ $rc -eq 0 ]]; then
      commit_needed_after_upgrade+=("INCLUDED")
    elif [[ $rc -eq 1 ]]; then
      commit_needed_after_upgrade+=("STILL_NEEDED")
    else
      commit_needed_after_upgrade+=("UNKNOWN")
    fi
  done
  echo ""
fi

# ── Print detailed report ─────────────────────────────────────────────────

print_separator() {
  printf '%0.s─' {1..100}
  echo ""
}

# Counters for summary
total_edits=0
total_backports=0
total_unknown=0
total_format_warnings=0
total_landed=0
total_not_landed=0
total_no_tag=0
total_drop=0

# Upgrade counters
total_included=0
total_still_needed=0
total_upgrade_drop=0
total_upgrade_review=0

echo "${c_bold}Detailed Commit Report${c_reset}"
print_separator

for ((i=0; i<${#commit_hashes[@]}; i++)); do
  hash="${commit_hashes[$i]}"
  subject="${commit_subjects[$i]}"
  ctype="${commit_types[$i]}"
  ticket="${commit_tickets[$i]}"
  expected="${commit_expected[$i]}"
  warning="${commit_warnings[$i]}"
  landed="${commit_landed[$i]}"

  # Type label with color
  case "$ctype" in
    EDIT)
      type_label="${c_cyan}[EDIT]${c_reset}"
      total_edits=$((total_edits + 1))
      ;;
    BACKPORT)
      type_label="${c_green}[BACKPORT]${c_reset}"
      total_backports=$((total_backports + 1))
      ;;
    UNKNOWN)
      type_label="${c_yellow}[UNKNOWN]${c_reset}"
      total_unknown=$((total_unknown + 1))
      ;;
  esac

  echo "${type_label}  ${c_dim}${hash:0:11}${c_reset}  ${subject}"

  # Print ticket and expected version info
  details=""
  if [[ -n "$ticket" ]]; then
    details="  Ticket: ${ticket}"
  fi
  if [[ -n "$expected" ]] && [[ "$expected" != DROP:* ]]; then
    details="${details}  |  Expected in: ${expected}"
  elif [[ "$expected" == DROP:* ]]; then
    details="${details}  |  Drop when upgrading to: ${expected#DROP:}"
  fi
  if [[ -n "$details" ]]; then
    echo "          ${c_dim}${details}${c_reset}"
  fi

  # Print landed status
  case "$landed" in
    LANDED)
      echo "          ${c_green}✓ Confirmed landed in ${expected}${c_reset}"
      total_landed=$((total_landed + 1))
      ;;
    NOT_LANDED)
      echo "          ${c_red}✗ NOT found in ${expected} — ticket may not have landed as expected${c_reset}"
      total_not_landed=$((total_not_landed + 1))
      ;;
    NO_TAG)
      echo "          ${c_yellow}? Tag rel/${expected} does not exist yet${c_reset}"
      total_no_tag=$((total_no_tag + 1))
      ;;
    DROP)
      echo "          ${c_yellow}↓ Marked for removal when upgrading to ${expected#DROP:}${c_reset}"
      total_drop=$((total_drop + 1))
      ;;
  esac

  # Print warnings
  if [[ -n "$warning" ]]; then
    echo "          ${c_red}⚠ ${warning}${c_reset}"
    total_format_warnings=$((total_format_warnings + 1))
  fi

  # Print upgrade recommendation
  if [[ -n "$UPGRADE_TO" ]]; then
    upgrade_status="${commit_needed_after_upgrade[$i]}"
    case "$upgrade_status" in
      INCLUDED)
        echo "          ${c_green}↑ Included in ${UPGRADE_TO} — can be dropped after upgrade${c_reset}"
        total_included=$((total_included + 1))
        ;;
      STILL_NEEDED)
        echo "          ${c_yellow}→ Still needed after upgrade to ${UPGRADE_TO}${c_reset}"
        total_still_needed=$((total_still_needed + 1))
        ;;
      DROP)
        echo "          ${c_green}↓ Marked for removal — drop after upgrade to ${UPGRADE_TO}${c_reset}"
        total_upgrade_drop=$((total_upgrade_drop + 1))
        ;;
      YES)
        echo "          ${c_yellow}→ HubSpot edit — will need to be re-applied after upgrade${c_reset}"
        total_still_needed=$((total_still_needed + 1))
        ;;
      REVIEW)
        echo "          ${c_yellow}? Needs manual review for upgrade to ${UPGRADE_TO}${c_reset}"
        total_upgrade_review=$((total_upgrade_review + 1))
        ;;
      UNKNOWN)
        echo "          ${c_yellow}? Could not determine — tag/branch for ${UPGRADE_TO} not available${c_reset}"
        total_upgrade_review=$((total_upgrade_review + 1))
        ;;
    esac
  fi

  echo ""
done

# ── Summary ────────────────────────────────────────────────────────────────
print_separator
echo ""
echo "${c_bold}Summary${c_reset}"
echo ""
echo "  Total commits:        ${COMMIT_COUNT}"
echo "  HubSpot Edits:        ${total_edits}"
echo "  HubSpot Backports:    ${total_backports}"
echo "  Unknown/Unclassified: ${total_unknown}"
echo ""
echo "  ${c_bold}Upstream verification:${c_reset}"
echo "    Confirmed landed:     ${total_landed}"
echo "    NOT landed as expected:${total_not_landed}"
echo "    Tag not yet available: ${total_no_tag}"
echo "    Marked for drop:       ${total_drop}"
echo "    Format warnings:       ${total_format_warnings}"

if [[ -n "$UPGRADE_TO" ]]; then
  echo ""
  echo "  ${c_bold}Upgrade to ${UPGRADE_TO}:${c_reset}"
  echo "    Included (can drop):  ${total_included}"
  echo "    Still needed:         ${total_still_needed}"
  echo "    Marked for drop:      ${total_upgrade_drop}"
  echo "    Needs review:         ${total_upgrade_review}"
fi

# ── Callouts ───────────────────────────────────────────────────────────────
echo ""
print_separator
echo ""
echo "${c_bold}Callouts${c_reset}"
echo ""

has_callouts=0

# 1. Unknown commits
if [[ $total_unknown -gt 0 ]]; then
  has_callouts=1
  echo "${c_yellow}⚠ ${total_unknown} commit(s) could not be classified.${c_reset}"
  echo "  These commits don't start with 'HubSpot Edit:' or 'HubSpot Backport:' and lack"
  echo "  upstream status annotations. Consider adding a proper prefix."
  echo ""
  for ((i=0; i<${#commit_hashes[@]}; i++)); do
    if [[ "${commit_types[$i]}" == "UNKNOWN" ]]; then
      echo "    ${c_dim}${commit_hashes[$i]:0:11}${c_reset}  ${commit_subjects[$i]}"
    fi
  done
  echo ""
fi

# 2. Format warnings
if [[ $total_format_warnings -gt 0 ]]; then
  has_callouts=1
  echo "${c_yellow}⚠ ${total_format_warnings} commit(s) have formatting issues.${c_reset}"
  echo "  Commit messages should start with 'HubSpot Edit:' or 'HubSpot Backport:'"
  echo "  to be properly classified."
  echo ""
  for ((i=0; i<${#commit_hashes[@]}; i++)); do
    if [[ -n "${commit_warnings[$i]}" ]]; then
      echo "    ${c_dim}${commit_hashes[$i]:0:11}${c_reset}  ${commit_subjects[$i]}"
      echo "      ${c_red}→ ${commit_warnings[$i]}${c_reset}"
    fi
  done
  echo ""
fi

# 3. Backports that didn't land
if [[ $total_not_landed -gt 0 ]]; then
  has_callouts=1
  echo "${c_red}✗ ${total_not_landed} backport(s) did NOT land in the expected upstream release.${c_reset}"
  echo "  These commits claim they will be in a specific release, but the JIRA ticket"
  echo "  was not found in that release's tag. They may have slipped or been deferred."
  echo ""
  for ((i=0; i<${#commit_hashes[@]}; i++)); do
    if [[ "${commit_landed[$i]}" == "NOT_LANDED" ]]; then
      echo "    ${c_dim}${commit_hashes[$i]:0:11}${c_reset}  ${commit_subjects[$i]}"
      echo "      ${c_red}→ Expected in ${commit_expected[$i]} but not found${c_reset}"
    fi
  done
  echo ""
fi

# 4. Backports that HAVE landed — can now be removed when rebasing
if [[ $total_landed -gt 0 ]]; then
  has_callouts=1
  echo "${c_green}✓ ${total_landed} backport(s) have landed upstream as expected.${c_reset}"
  echo "  These can be dropped when rebasing onto the release that includes them."
  echo ""
  for ((i=0; i<${#commit_hashes[@]}; i++)); do
    if [[ "${commit_landed[$i]}" == "LANDED" ]]; then
      echo "    ${c_dim}${commit_hashes[$i]:0:11}${c_reset}  ${commit_subjects[$i]}"
      echo "      ${c_green}→ Landed in ${commit_expected[$i]}${c_reset}"
    fi
  done
  echo ""
fi

# 5. Upgrade recommendations
if [[ -n "$UPGRADE_TO" ]] && [[ $total_included -gt 0 ]]; then
  has_callouts=1
  echo "${c_green}↑ ${total_included} backport(s) are included in ${UPGRADE_TO} and can be dropped.${c_reset}"
  echo ""
  for ((i=0; i<${#commit_hashes[@]}; i++)); do
    if [[ -n "${commit_needed_after_upgrade[$i]}" ]] && [[ "${commit_needed_after_upgrade[$i]}" == "INCLUDED" ]]; then
      echo "    ${c_dim}${commit_hashes[$i]:0:11}${c_reset}  ${commit_subjects[$i]}"
    fi
  done
  echo ""
fi

if [[ $has_callouts -eq 0 ]]; then
  echo "  No issues found. All commits are properly classified and verified."
fi

echo ""
echo "${c_dim}Report generated at $(date)${c_reset}"
