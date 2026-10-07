// 注册密码规则（与 internal/account.ValidatePassword 同一口径）：8–16 个字符（按码点计），
// 数字、大写字母、小写字母三类中至少两类。登录不校验规则。
export const PASSWORD_MIN = 8;
export const PASSWORD_MAX = 16;
export const PASSWORD_RULE_TEXT = "密码须为 8–16 位，且至少包含数字、大写字母、小写字母中的两种";

export interface PasswordChecks {
  length: boolean;
  classes: boolean;
}

export function passwordChecks(pw: string): PasswordChecks {
  const n = [...pw].length;
  const kinds = [/[0-9]/, /[A-Z]/, /[a-z]/].filter((re) => re.test(pw)).length;
  return { length: n >= PASSWORD_MIN && n <= PASSWORD_MAX, classes: kinds >= 2 };
}

export function passwordError(pw: string): string {
  const c = passwordChecks(pw);
  return c.length && c.classes ? "" : PASSWORD_RULE_TEXT;
}
