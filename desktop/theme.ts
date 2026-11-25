// Design tokens for the desktop companion.
//
// Every colour, surface, focus ring and motion class the window uses is named
// here once. `theme` is what App.tsx imports; `dark` is the same token set for a
// dark appearance and is exported unwired, because NSH reports the appearance
// through a host API this app does not use yet. Switching is one line:
// `export const theme = dark`.

import type { Tone } from "./snapshot";

const light = {
  canvas: "bg-[#f3f5f9]",
  surface: "bg-white",
  border: "border-[#d8dfe8]",

  rail: "bg-[#212635]",
  railTitle: "text-white",
  railItem: "text-[#c5ccdd]",
  railItemHover: "hover:bg-[#33394a] active:bg-[#444c60]",
  railItemSelected: "bg-[#dce7fb] active:bg-[#c6d8f7]",
  railItemSelectedText: "text-[#192843]",

  heading: "text-[#1d2938]",
  value: "text-[#1d2938]",
  label: "text-[#526174]",
  muted: "text-[#526174]",
  secondary: "text-[#354459]",

  button: "border-[#78889d] bg-white hover:bg-[#eef2f6] active:bg-[#dce7fb]",
  buttonDisabled: "border-[#d8dfe8] bg-[#eef2f6]",
  buttonDisabledText: "text-[#526174]",

  chip: "border-[#d8dfe8] bg-white hover:bg-[#eef2f6]",
  chipText: "text-[#354459]",
  chipSelected: "border-[#1d2938] bg-[#1d2938]",
  chipSelectedText: "text-white",

  errorSurface: "border-[#efd2c8] bg-[#fff3ee]",
  errorText: "text-[#913c27]",
  demoSurface: "border-[#f0d9a8] bg-[#fff0d2]",
  demoText: "text-[#764608]",

  spinner: "border-[#9ebffb] border-t-transparent",

  tone: {
    attention: "text-[#764608]",
    pending: "text-[#1756ad]",
    ok: "text-[#17614c]",
    neutral: "text-[#526174]",
  } as Record<Tone, string>,
  pill: {
    attention: "bg-[#fff0d2] text-[#764608]",
    pending: "bg-[#dce7fb] text-[#192843]",
    ok: "bg-[#e4f2ec] text-[#17614c]",
    neutral: "bg-[#eef2f6] text-[#354459]",
  } as Record<Tone, string>,

  focus: { outlineWidth: 2, outlineOffset: 2, outlineColor: "#1756ad" },
  navFocus: { outlineWidth: 2, outlineOffset: 2, outlineColor: "#9ebffb" },

  press: "transition duration-100",
  spin: "motion-safe:animate-spin",
  pulse: "motion-safe:animate-pulse",
};

export type Theme = typeof light;

export const dark: Theme = {
  canvas: "bg-[#0f1218]",
  surface: "bg-[#171b24]",
  border: "border-[#2b3243]",

  rail: "bg-[#0b0e14]",
  railTitle: "text-[#eef1f7]",
  railItem: "text-[#aeb7c8]",
  railItemHover: "hover:bg-[#1d2331] active:bg-[#262e3f]",
  railItemSelected: "bg-[#dce7fb] active:bg-[#c6d8f7]",
  railItemSelectedText: "text-[#192843]",

  heading: "text-[#eef1f7]",
  value: "text-[#e6e9f0]",
  label: "text-[#98a2b4]",
  muted: "text-[#98a2b4]",
  secondary: "text-[#c2cad8]",

  button: "border-[#5b6a82] bg-[#171b24] hover:bg-[#1d2331] active:bg-[#262e3f]",
  buttonDisabled: "border-[#2b3243] bg-[#141821]",
  buttonDisabledText: "text-[#98a2b4]",

  chip: "border-[#2b3243] bg-[#171b24] hover:bg-[#1d2331]",
  chipText: "text-[#c2cad8]",
  chipSelected: "border-[#dce7fb] bg-[#dce7fb]",
  chipSelectedText: "text-[#192843]",

  errorSurface: "border-[#5d2f24] bg-[#2a1a16]",
  errorText: "text-[#f0a58c]",
  demoSurface: "border-[#5a4415] bg-[#2a2313]",
  demoText: "text-[#f2c46b]",

  spinner: "border-[#33507f] border-t-transparent",

  tone: {
    attention: "text-[#f2c46b]",
    pending: "text-[#9ebffb]",
    ok: "text-[#7fd6b0]",
    neutral: "text-[#98a2b4]",
  },
  pill: {
    attention: "bg-[#3a2f16] text-[#f2c46b]",
    pending: "bg-[#1e2b45] text-[#9ebffb]",
    ok: "bg-[#14312a] text-[#7fd6b0]",
    neutral: "bg-[#232a38] text-[#c2cad8]",
  },

  focus: { outlineWidth: 2, outlineOffset: 2, outlineColor: "#9ebffb" },
  navFocus: { outlineWidth: 2, outlineOffset: 2, outlineColor: "#9ebffb" },

  press: "transition duration-100",
  spin: "motion-safe:animate-spin",
  pulse: "motion-safe:animate-pulse",
};

export const theme = light;
