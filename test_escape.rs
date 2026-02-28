fn main() {
    let text1 = "Special chars: _ * [ ] ( ) ~ ` > # + - = | { } . !";
    match telegram_markdown_v2::convert_with_strategy(text1, telegram_markdown_v2::UnsupportedTagsStrategy::Escape) {
        Ok(rendered) => println!("Rendered: {}", rendered),
        Err(err) => println!("Error: {}", err),
    }
}
