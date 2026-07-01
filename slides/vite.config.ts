import { defineConfig } from 'vite'

// Slidev 52.16+ (rolldown-vite) tightened the slide-import-guard: public
// assets referenced as `/img/...` under `--base ./` get rejected as
// "outside of server.fs.allow". Widen the allow-list to the project (and its
// parent) so the deck's public/img assets resolve for both `dev` and `build`.
export default defineConfig({
  server: {
    fs: {
      strict: false,
      allow: ['..', '.'],
    },
  },
})
