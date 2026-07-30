from __future__ import annotations

import pytest

from synor._internal.revocation_model import (
    InvalidRevocationTransition,
    RevocationStage,
    transition_case,
)

from ._fixtures import make_case


def test_acknowledgement_cannot_close_without_verification() -> None:
    case = make_case()
    for stage in (
        RevocationStage.SUPPRESSED,
        RevocationStage.PLANNED,
        RevocationStage.DISPATCHED,
        RevocationStage.ACKNOWLEDGED,
    ):
        case = transition_case(case, stage)

    assert case.stage is RevocationStage.ACKNOWLEDGED
    with pytest.raises(InvalidRevocationTransition):
        transition_case(case, RevocationStage.CLOSED)
