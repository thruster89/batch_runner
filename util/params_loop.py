from itertools import product
from util.param_expand import expand_param_value
from oracle.sql_utils import extract_params  # ← 네 기존 함수


def build_param_cases_for_sql(
    sql_text: str,
    global_params: dict,
) -> list[dict]:
    """
    SQL에 실제로 등장한 파라미터만 대상으로
    실행 케이스(dict)를 생성
    """
    used_keys = extract_params(sql_text)

    # SQL에 파라미터가 아예 없으면 1회만 실행
    if not used_keys:
        return [global_params.copy()]

    keys = []
    values = []

    for k in used_keys:
        if k not in global_params:
            raise RuntimeError(f"Missing param '{k}' required by SQL")

        keys.append(k)
        values.append(
            expand_param_value(str(global_params[k]))
        )

    cases = []
    for combo in product(*values):
        case = global_params.copy()
        case.update(dict(zip(keys, combo)))
        cases.append(case)

    return cases
