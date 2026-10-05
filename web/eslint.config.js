import tseslint from "typescript-eslint";
import pluginVue from "eslint-plugin-vue";

export default tseslint.config(
  { ignores: ["dist/**", "node_modules/**", "src/api/schema.d.ts"] },
  ...tseslint.configs.recommended,
  ...pluginVue.configs["flat/recommended"],
  {
    files: ["**/*.vue"],
    languageOptions: { parserOptions: { parser: tseslint.parser } },
  },
  {
    rules: {
      "vue/max-attributes-per-line": "off",
      "vue/singleline-html-element-content-newline": "off",
      "vue/html-self-closing": "off",
      "no-restricted-globals": ["error", { name: "localStorage", message: "token 不得写入 localStorage（§15.3）" }],
    },
  },
  {
    files: ["**/*.test.ts"],
    rules: { "no-restricted-globals": "off" },
  },
);
