use super::*;

#[test]
fn html_bold_and_italic() {
    let text = "This is **bold** and *italic* text.";
    let rendered = render_html_reply(text);
    assert!(rendered.contains("<b>bold</b>"));
    assert!(rendered.contains("<i>italic</i>"));
}

#[test]
fn html_inline_code() {
    let text = "Run `echo hello` in the terminal.";
    let rendered = render_html_reply(text);
    assert!(rendered.contains("<code>echo hello</code>"));
}

#[test]
fn html_code_block_with_language() {
    let text = "```rust\nfn main() {}\n```";
    let rendered = render_html_reply(text);
    assert!(rendered.contains("<pre><code class=\"language-rust\">"));
    assert!(rendered.contains("fn main() {}"));
    assert!(rendered.contains("</code></pre>"));
}

#[test]
fn html_escapes_special_chars() {
    let text = "x < 10 & y > 5";
    let rendered = render_html_reply(text);
    assert!(rendered.contains("x &lt; 10 &amp; y &gt; 5"));
}

#[test]
fn html_unclosed_bold_does_not_crash() {
    let text = "This is **unclosed bold";
    let rendered = render_html_reply(text);
    // pulldown-cmark handles unclosed tags gracefully
    assert!(!rendered.is_empty());
}

#[test]
fn html_link() {
    let text = "Visit [example](https://example.com) now.";
    let rendered = render_html_reply(text);
    assert!(rendered.contains("<a href=\"https://example.com\">example</a>"));
}

#[test]
fn html_unordered_list() {
    let text = "Items:\n- apple\n- banana";
    let rendered = render_html_reply(text);
    assert!(rendered.contains("• apple"));
    assert!(rendered.contains("• banana"));
}

#[test]
fn html_ordered_list() {
    let text = "Steps:\n1. first\n2. second";
    let rendered = render_html_reply(text);
    assert!(rendered.contains("1. first"));
    assert!(rendered.contains("2. second"));
}

#[test]
fn html_heading_renders_as_bold() {
    let text = "# Hello World";
    let rendered = render_html_reply(text);
    assert!(rendered.contains("<b>Hello World</b>"));
}

#[test]
fn html_cjk_with_markdown() {
    let text = "使用 **antigravity** 的 `cli` 工具";
    let rendered = render_html_reply(text);
    assert!(rendered.contains("<b>antigravity</b>"));
    assert!(rendered.contains("<code>cli</code>"));
}

#[test]
fn html_preserves_xml_tags() {
    let text = "Here is some <boltArtifact>content</boltArtifact> and <thinking>stuff</thinking>";
    let rendered = render_html_reply(text);
    assert!(rendered.contains("&lt;boltArtifact&gt;"));
    assert!(rendered.contains("&lt;/boltArtifact&gt;"));
    assert!(rendered.contains("&lt;thinking&gt;"));
    assert!(rendered.contains("&lt;/thinking&gt;"));
}

#[test]
fn test_html_escape() {
    assert_eq!(html_escape("a & b"), "a &amp; b");
    assert_eq!(html_escape("a < b"), "a &lt; b");
    assert_eq!(html_escape("a > b"), "a &gt; b");
    assert_eq!(html_escape("&<>"), "&amp;&lt;&gt;");
    assert_eq!(html_escape(""), "");
    assert_eq!(html_escape("no special chars"), "no special chars");
    assert_eq!(html_escape("already &amp;"), "already &amp;amp;");
}

#[test]
fn valid_telegram_chat_id_falls_back_to_allowlist() {
    let mut cfg = RuntimeConfig::test_config();
    cfg.telegram_chat_id = None;
    cfg.telegram_chat_ids = vec!["".to_string(), "999".to_string(), "321".to_string()];

    assert_eq!(valid_telegram_chat_id(&cfg), Some("999"));
}

#[test]
fn telegram_not_modified_error_is_treated_as_idempotent() {
    let err =
        anyhow::anyhow!("telegram editMessageText failed: Bad Request: message is not modified");
    assert!(telegram_error_is_not_modified(&err));
}

#[test]
fn unrelated_telegram_error_is_not_treated_as_not_modified() {
    let err = anyhow::anyhow!("telegram editMessageText failed: Bad Request: message not found");
    assert!(!telegram_error_is_not_modified(&err));
}
