// @vitest-environment jsdom
import { afterEach, describe, expect, it, vi } from "vitest";
import { TokenStore, authHeaders } from "./auth";

afterEach(() => {
  sessionStorage.clear();
  localStorage.clear();
});

describe("TokenStore", () => {
  it("keeps the token in memory only by default and never touches localStorage", () => {
    const setLocal = vi.spyOn(Storage.prototype, "setItem");
    const s = new TokenStore();
    s.set("tok-1");
    expect(s.get()).toBe("tok-1");
    expect(s.persistence()).toBe("memory");
    expect(setLocal).not.toHaveBeenCalled();
    expect(localStorage.length).toBe(0);
    expect(sessionStorage.length).toBe(0);
    expect(new TokenStore().get()).toBe("");
  });

  it("optionally persists to sessionStorage and restores from it, never localStorage", () => {
    const s = new TokenStore();
    s.set("tok-2", "session");
    expect(localStorage.length).toBe(0);
    const restored = new TokenStore();
    expect(restored.get()).toBe("tok-2");
    expect(restored.persistence()).toBe("session");
    restored.set("tok-2", "memory");
    expect(sessionStorage.length).toBe(0);
    restored.clear();
    expect(restored.get()).toBe("");
  });

  it("builds only an Authorization header", () => {
    expect(authHeaders("abc")).toEqual({ Authorization: "Bearer abc" });
    expect(authHeaders("")).toEqual({});
  });
});
